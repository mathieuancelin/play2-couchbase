package org.ancelin.play2.java.couchbase;

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.ops.OperationStatus;
import org.ancelin.play2.couchbase.Couchbase$;
import play.Play;
import play.libs.F;
import play.libs.F.Promise;
import play.libs.Json;
import scala.concurrent.ExecutionContext;

import java.util.Collection;


public class CouchbaseBucket {

    public final org.ancelin.play2.couchbase.CouchbaseBucket client;

    private final Couchbase$ couchbase = Couchbase$.MODULE$;
    private final ExecutionContext ec = couchbase.couchbaseExecutor(Play.application().getWrappedApplication());

    CouchbaseBucket(org.ancelin.play2.couchbase.CouchbaseBucket client) {
        this.client = client;
    }

    public <T> Promise<Collection<T>> find(String docName, String viewName, Query query, Class<T> clazz) {
        return new Promise<Collection<T>>(couchbase.javaFind(docName, viewName, query, clazz, client, ec));
    }

    public <T> Promise<Collection<T>> find(View view, Query query, Class<T> clazz) {
        return new Promise<Collection<T>>(couchbase.javaFind(view, query, clazz, client, ec));
    }

    public Promise<View> view(String docName, String view) {
        return new Promise<View>(couchbase.javaView(docName, view, client, ec));
    }

    public <T> Promise<T> get(String key, Class<T> clazz) {
        return new Promise<T>(couchbase.javaGet(key, clazz, client, ec));
    }

    public <T> Promise<F.Option<T>> getopt(String key, Class<T> clazz) {
        return new Promise<F.Option<T>>(couchbase.javaOptGet(key, clazz, client, ec));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Set Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public <T> Promise<OperationStatus> set(String key, T value) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, -1, Json.stringify(Json.toJson(value)), PersistTo.ZERO, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, int exp, T value) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, exp, Json.stringify(Json.toJson(value)), PersistTo.ZERO, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, int exp, T value, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, exp, Json.stringify(Json.toJson(value)), PersistTo.ZERO, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, T value, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, -1, Json.stringify(Json.toJson(value)), PersistTo.ZERO, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, int exp, T value, PersistTo persistTo) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, exp, Json.stringify(Json.toJson(value)), persistTo, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, T value, PersistTo persistTo) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, -1, Json.stringify(Json.toJson(value)), persistTo, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, int exp, T value, PersistTo persistTo, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, exp, Json.stringify(Json.toJson(value)), persistTo, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> set(String key, T value, PersistTo persistTo, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaSet(key, -1, Json.stringify(Json.toJson(value)), persistTo, replicateTo, client, ec));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public <T> Promise<OperationStatus> add(String key, T value) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, -1, Json.stringify(Json.toJson(value)), PersistTo.ZERO, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, int exp, T value) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, exp, Json.stringify(Json.toJson(value)), PersistTo.ZERO, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, int exp, T value, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, exp, Json.stringify(Json.toJson(value)), PersistTo.ZERO, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, T value, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, -1, Json.stringify(Json.toJson(value)), PersistTo.ZERO, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, int exp, T value, PersistTo persistTo) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, exp, Json.stringify(Json.toJson(value)), persistTo, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, T value, PersistTo persistTo) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, -1, Json.stringify(Json.toJson(value)), persistTo, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, int exp, T value, PersistTo persistTo, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, exp, Json.stringify(Json.toJson(value)), persistTo, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> add(String key, T value, PersistTo persistTo, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaAdd(key, -1, Json.stringify(Json.toJson(value)), persistTo, replicateTo, client, ec));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Replace Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public <T> Promise<OperationStatus> replace(String key, int exp, T value) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, exp, Json.stringify(Json.toJson(value)), PersistTo.ZERO, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, int exp, T value, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, exp, Json.stringify(Json.toJson(value)), PersistTo.ZERO, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, T value, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, -1, Json.stringify(Json.toJson(value)), PersistTo.ZERO, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, int exp, T value, PersistTo persistTo) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, exp, Json.stringify(Json.toJson(value)), persistTo, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, T value, PersistTo persistTo) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, -1, Json.stringify(Json.toJson(value)), persistTo, ReplicateTo.ZERO, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, int exp, T value, PersistTo persistTo, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, exp, Json.stringify(Json.toJson(value)), persistTo, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, T value, PersistTo persistTo, ReplicateTo replicateTo) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, -1, Json.stringify(Json.toJson(value)), persistTo, replicateTo, client, ec));
    }

    public <T> Promise<OperationStatus> replace(String key, T value) {
        return new Promise<OperationStatus>(couchbase.javaReplace(key, -1, Json.stringify(Json.toJson(value)), PersistTo.ZERO, ReplicateTo.ZERO, client, ec));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Promise<OperationStatus> delete(String key){
        return new Promise<OperationStatus>(couchbase.delete(key, client, ec));
    }

    public Promise<OperationStatus> delete(String key, ReplicateTo replicateTo){
        return new Promise<OperationStatus>(couchbase.delete(key, replicateTo, client, ec));
    }

    public Promise<OperationStatus> delete(String key, PersistTo persistTo){
        return new Promise<OperationStatus>(couchbase.delete(key, persistTo, client, ec));
    }

    public Promise<OperationStatus> delete(String key, PersistTo persistTo, ReplicateTo replicateTo){
        return new Promise<OperationStatus>(couchbase.delete(key, persistTo, replicateTo, client, ec));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Flush Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Promise<OperationStatus> flush(int delay) {
        return new Promise<OperationStatus>(couchbase.flush(delay, client, ec));
    }

    public Promise<OperationStatus> flush() {
       return new Promise<OperationStatus>(couchbase.flush(client, ec));
    }
}
