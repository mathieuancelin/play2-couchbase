package org.ancelin.play2.couchbase.store

import akka.actor.{ActorSystem, Props, Actor, ActorRef}
import akka.pattern.ask
import java.util.concurrent.{TimeUnit, ConcurrentHashMap}
import org.ancelin.play2.couchbase.{Couchbase, CouchbaseBucket}
import scala.concurrent.{Future, Await, ExecutionContext}
import play.api.libs.json.{JsSuccess, JsValue, Format, Json}
import play.api.Play
import com.couchbase.client.protocol.views.{ComplexKey, Stale, Query}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import java.util.UUID

case class Message(payload: Any, eventId: Long = 0L, aggregateId: Long = 0L, timestamp: Long = System.currentTimeMillis(), version: Int = 0)
case class CouchbaseMessage(messageKey: String, blobKey: String, eventId: Long = 0L, aggregateId: Long = 0L, timestamp: Long = System.currentTimeMillis(), version: Int = 0, datatype: String = "eventsourcing-message", blobClass: String, blob: JsValue)
case class CouchbaseSnapshotState(snapshotKey: String, snapshotId: String, timestamp: Long = System.currentTimeMillis(), datatype: String = "eventsourcing-snapshot-state", blobClass: String, blob: JsValue)

object Message {
  def create(payload: Any) = Message(payload, 0L, 0L, System.currentTimeMillis(), 0)
  def create(payload: Any, id: Long) = Message(payload, id, 0L, System.currentTimeMillis(), 0)
  def create(payload: Any, id: Long, aggregate: Long) = Message(payload, id, aggregate, System.currentTimeMillis(), 0)
  def create(payload: Any, id: Long, aggregate: Long, version: Int) = Message(payload, id, aggregate, System.currentTimeMillis(), version)
}

case class WriteInJournal(message: Message, replyTo: ActorRef, journal: CouchbaseEventSourcing)
case class WrittenInJournal(message: Message)
case class Replay(message: Message)
case class SnapshotRequest(snapshotId: String, journal: CouchbaseEventSourcing) {
  def store(state: Any) = {
    val timestamp = System.currentTimeMillis()
    val uuid = UUID.randomUUID().toString
    val key = s"eventsourcing-snapshot-state-$uuid-$timestamp"
    journal.snapshotFormatters.get(state.getClass.getName).map { formatter =>
      val blobAsJson = formatter.asInstanceOf[Format[Any]].writes(state)
      val snap = CouchbaseSnapshotState(key, snapshotId, timestamp, "eventsourcing-snapshot-state", state.getClass.getName, blobAsJson);
      Couchbase.set(key, snap)(journal.theBucket(), CouchbaseEventSourcing.formatSnap, journal.ec)
    }.getOrElse(throw new RuntimeException(s"Can't find formatter for class ${state.getClass.getName}"))
  }
}
case class SnapshotState(state: Any)

trait EventStored extends Actor {

  private val couchbaseJournal = CouchbaseEventSourcing(context.system)

  abstract override def receive = {
    case msg: Message => {
      couchbaseJournal.journal.forward(WriteInJournal(msg, self, couchbaseJournal))
    }
    case WrittenInJournal(msg) => {
      super.receive(msg)
      super.receive(msg.payload)
    }
    case Replay(msg) => {
      super.receive(msg)
      super.receive(msg.payload)
    }
  }

  override def postStop() {
    couchbaseJournal.actors = couchbaseJournal.actors.filter( _ != self )
  }
}

private class CouchbaseJournalActor(bucket: CouchbaseBucket, format: Format[CouchbaseMessage], ec: ExecutionContext) extends Actor {
  def receive = {
    case WriteInJournal(msg, to, journal) => {
      val blobKey = s"eventsourcing-message-${msg.eventId}-${msg.aggregateId}-${msg.timestamp}-blob"
      val dataKey = s"eventsourcing-message-${msg.eventId}-${msg.aggregateId}-${msg.timestamp}-data"
      journal.eventFormatters.get(msg.payload.getClass.getName).map { formatter =>
        val blobAsJson = formatter.asInstanceOf[Format[Any]].writes(msg.payload)
        val dataMsg = CouchbaseMessage(dataKey, blobKey, msg.eventId, msg.aggregateId, msg.timestamp, msg.version, "eventsourcing-message", msg.payload.getClass.getName, blobAsJson)
        Couchbase.set(dataKey, dataMsg)(bucket, format, ec)
          .map(_ => to ! WrittenInJournal(msg))(ec)
      }.getOrElse(throw new RuntimeException(s"Can't find formatter for class ${msg.payload.getClass.getName}"))
    }
    case _ =>
  }
}

class CouchbaseEventSourcing(system: ActorSystem, bucket: CouchbaseBucket, format: Format[CouchbaseMessage], snapFormat: Format[CouchbaseSnapshotState]) {
  implicit val ec = Couchbase.couchbaseExecutor(Play.current)
  val journal: ActorRef = system.actorOf(Props(new CouchbaseJournalActor(bucket, format, ec)))
  var actors: List[ActorRef] = List[ActorRef]()
  private val byDataType = Couchbase.view("event-sourcing", "datatype")(bucket, ec)
  private val byTimestamp = Couchbase.view("event-sourcing", "by_timestamp")(bucket, ec)
  private val byEventId = Couchbase.view("event-sourcing", "by_eventid")(bucket, ec)
  private val byAggregateId = Couchbase.view("event-sourcing", "by_aggregateid")(bucket, ec)
  private val byVersion = Couchbase.view("event-sourcing", "by_version")(bucket, ec)
  private val bySnapshot = Couchbase.view("event-sourcing", "states_by_snapshot")(bucket, ec)
  private val bySnapshotTimestamp = Couchbase.view("event-sourcing", "states_by_timestamp")(bucket, ec)

  private val all = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false)
  private def allFrom(from: Long) = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setRangeStart(ComplexKey.of(from.asInstanceOf[AnyRef])).setRangeEnd(ComplexKey.of(Long.MaxValue.asInstanceOf[AnyRef]))
  private def allUntil(to: Long) = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setRangeStart(ComplexKey.of(0L.asInstanceOf[AnyRef])).setRangeEnd(ComplexKey.of(to.asInstanceOf[AnyRef]))
  private def allUntilDesc(to: Long) = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(true).setRangeStart(ComplexKey.of(0L.asInstanceOf[AnyRef])).setRangeEnd(ComplexKey.of(to.asInstanceOf[AnyRef]))
  private def snapshot(id: String) = new Query().setIncludeDocs(true).setStale(Stale.FALSE).setDescending(false).setRangeStart(ComplexKey.of(id)).setRangeEnd(ComplexKey.of(id + "\uefff"))
  def theBucket():CouchbaseBucket = bucket
  var eventFormatters: Map[String, Format[_]] = Map()
  var snapshotFormatters: Map[String, Format[_]] = Map()

  def registerEventFormatter[T](formatter: Format[T])(implicit tag: ClassTag[T]) = {
    eventFormatters = eventFormatters + (tag.runtimeClass.getName -> formatter)
    this
  }

  def registerSnapshotFormatter[T](formatter: Format[T])(implicit tag: ClassTag[T]) = {
    snapshotFormatters = snapshotFormatters + (tag.runtimeClass.getName -> formatter)
    this
  }

  def processorOf(props: Props): ActorRef = {
    val actorRef = system.actorOf(props)
    actors = actors :+ actorRef
    actorRef
  }

  def processorOf(props: Props, name: String): ActorRef = {
    val actorRef = system.actorOf(props, name)
    actors = actors :+ actorRef
    actorRef
  }

  private def replayEvent(message: CouchbaseMessage) = {
    eventFormatters.get(message.blobClass).map { formatter =>
      formatter.reads(message.blob) match {
        case s: JsSuccess[_] => {
          val msg = Message(s.get, message.eventId, message.aggregateId, message.timestamp, message.version)
          actors.foreach { actor =>
            actor ! Replay(msg)
          }
        }
        case _ => throw new RuntimeException(s"Can't read blob for class ${message.blobClass} : ${Json.stringify(message.blob)}")
      }
    }.getOrElse(throw new RuntimeException(s"Can't find formatter for class ${message.blobClass}"))
  }

  def replayAll() = {
    byTimestamp.flatMap { view =>
      Couchbase.find[CouchbaseMessage](view)(all)(bucket, format, ec)
    }.map(_.map(replayEvent))
  }

  def replayFrom(timestamp: Long) = {
    byTimestamp.flatMap { view =>
      Couchbase.find[CouchbaseMessage](view)(allFrom(timestamp))(bucket, format, ec)
    }.map(_.map(replayEvent))
  }

  def replayUntil(timestamp: Long) = {
    byTimestamp.flatMap { view =>
      Couchbase.find[CouchbaseMessage](view)(allUntil(timestamp))(bucket, format, ec)
    }.map(_.map(replayEvent))
  }

  def replayFromId(id: Long) = {
    byEventId.flatMap { view =>
      Couchbase.find[CouchbaseMessage](view)(allFrom(id))(bucket, format, ec)
    }.map(_.map(replayEvent))
  }

  def replayUntilId(id: Long) = {
    byEventId.flatMap { view =>
      Couchbase.find[CouchbaseMessage](view)(allUntil(id))(bucket, format, ec)
    }.map(_.map(replayEvent))
  }

  def createSnapshot(): Future[String] = {
    val uuid = UUID.randomUUID().toString
    Future.sequence(actors.map { actor =>
      actor ? SnapshotRequest(uuid, this)
    }).map { sequence =>
      uuid
    }
  }

  def recoverFromSnapshot(id: String) = Future[Boolean] = {
    bySnapshot.flatMap { view =>
      Couchbase.find[CouchbaseSnapshotState](view)(snapshot(id))(bucket, snapFormat, ec).flatMap { list =>
        list.headOption.map { state =>
          snapshotFormatters.get(state.blobClass).map { formatter =>
            formatter.reads(state.blob) match {
              case s: JsSuccess[_] => {
                Future.sequence(actors.map { actor =>
                  actor ? SnapshotState(state)
                }).map { sequence =>
                  true
                }
              }
              case _ => throw new RuntimeException(s"Can't read blob for class ${state.blobClass} : ${Json.stringify(state.blob)}")
            }
          }.getOrElse(throw new RuntimeException(s"Can't find formatter for class ${state.blobClass}"))
        }.getOrElse(throw new RuntimeException(s"Can't find snapshot with id $id"))
      }
    }
  }

  def recoverFromLastSnapshot() = Future[Boolean] = {
    bySnapshotTimestamp.flatMap { view =>
      Couchbase.find[CouchbaseSnapshotState](view)(allUntilDesc(System.currentTimeMillis()))(bucket, snapFormat, ec).flatMap { list =>
        list.headOption.map { state =>
          snapshotFormatters.get(state.blobClass).map { formatter =>
            formatter.reads(state.blob) match {
              case s: JsSuccess[_] => {
                Future.sequence(actors.map { actor =>
                  actor ? SnapshotState(state)
                }).map { sequence =>
                  true
                }
              }
              case _ => throw new RuntimeException(s"Can't read blob for class ${state.blobClass} : ${Json.stringify(state.blob)}")
            }
          }.getOrElse(throw new RuntimeException(s"Can't find formatter for class ${state.blobClass}"))
        }.getOrElse(throw new RuntimeException(s"Can't find snapshot"))
      }
    }
  }
}

object CouchbaseEventSourcing {
  val format = Json.format[CouchbaseMessage]
  val formatSnap = Json.format[CouchbaseSnapshotState]
  val journals: ConcurrentHashMap[String, CouchbaseEventSourcing] = new ConcurrentHashMap[String, CouchbaseEventSourcing]()
  def apply(system: ActorSystem, bucket: CouchbaseBucket) = {
    if (!journals.containsKey(system.name)) {
      // TODO : check if view are here. If not, insert them
      val ec = Couchbase.couchbaseExecutor(Play.current)
      val eventSourcingDesignDoc =
        """ {
            "views":{
               "datatype": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-message') { emit(doc.id, null); } } "
               },
               "by_timestamp": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-message') { emit(doc.timestamp, null); } } "
               },
               "by_version": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-message') { emit(doc.version, null); } } "
               },
               "by_aggregateId": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-message') { emit(doc.aggregateId, null); } } "
               },
               "by_eventId": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-message') { emit(doc.eventId, null); } } "
               },
               "states_by_snapshot": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-snapshot-state') { emit(doc.snapshotId, null); } } "
               },
               "states_by_timestamp": {
                   "map": "function (doc, meta) { if (doc.datatype === 'eventsourcing-snapshot-state') { emit(doc.timestamp, null); } } "
               }
            }
        } """
      try {
        Await.result(Couchbase.createDesignDoc("event-sourcing", eventSourcingDesignDoc)(bucket, ec), Duration(2, TimeUnit.SECONDS))
      } catch {
        case e => println(e)
      }
      journals.putIfAbsent(system.name, new CouchbaseEventSourcing(system, bucket, format, formatSnap))
    }
    journals.get(system.name)
  }
  def apply(system: ActorSystem) = {
    journals.get(system.name)
  }
}
