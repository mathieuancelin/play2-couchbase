package org.ancelin.play2.java.couchbase;

import com.couchbase.client.CouchbaseClient;
import org.ancelin.play2.couchbase.CouchbasePlugin;
import play.Play;
import play.api.PlayException;
import play.libs.F;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.HashMap;
import java.util.Map;

public class Couchbase {

    private static CouchbasePlugin plugin = Play.application().plugin(CouchbasePlugin.class);
    private static String initMessage = "The CouchbasePlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:package org.ancelin.play2.couchbase.CouchbasePlugin' (400 is an arbitrary priority and may be changed to match your needs).";
    private static String connectMessage = "The CouchbasePlugin doesn't seems to be connected to a Couchbase server. Maybe an error occured!";

    public static Map<String, CouchbaseClient> buckets() {
        Map<String, CouchbaseClient> buckets = new HashMap<String, CouchbaseClient>();
        Iterator<Tuple2<String,org.ancelin.play2.couchbase.Couchbase>> iterator = plugin.buckets().iterator();
        while(iterator.hasNext()) {
            Tuple2<String,org.ancelin.play2.couchbase.Couchbase> tuple = iterator.next();
            buckets.put(tuple._1(), tuple._2().client().get());
        }
        return buckets;
    }

    public static CouchbaseClient bucket(String name) {
        Option<org.ancelin.play2.couchbase.Couchbase> opt = plugin.buckets().get(name);
        if (opt.isDefined()) {
            return opt.get().client().get();
        }
        throw new PlayException("CouchbasePlugin Error", initMessage);
    }

    public static CouchbaseClient defaultBucket() {
        Option<Tuple2<String,org.ancelin.play2.couchbase.Couchbase>> tuple2Option = plugin.buckets().headOption();
        if (tuple2Option.isDefined()) {
            return tuple2Option.get()._2().client().get();
        }
        throw new PlayException("CouchbasePlugin Error", connectMessage);
    }

    public static <T> T withCouchbase(F.Function<CouchbaseAPI, T> block) {
        try {
            return block.apply(new CouchbaseAPI(defaultBucket()));
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public static <T> T withCouchbase(String bucket, F.Function<CouchbaseAPI, T> block) {
        try {
            return block.apply(new CouchbaseAPI(bucket(bucket)));
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
