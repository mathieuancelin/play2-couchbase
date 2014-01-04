package org.ancelin.play2.couchbase.client

import play.api.libs.json.{Json, JsObject}
import net.spy.memcached.ops.OperationStatus
import java.lang.RuntimeException

class JsonValidationException(message: String, errors: JsObject) extends RuntimeException(message + " : " + Json.stringify(errors))
class OperationFailedException(status: OperationStatus) extends RuntimeException(status.getMessage)