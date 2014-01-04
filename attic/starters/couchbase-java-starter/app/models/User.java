package models;

import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import net.spy.memcached.ops.OperationStatus;
import org.ancelin.play2.java.couchbase.Couchbase;
import org.ancelin.play2.java.couchbase.CouchbaseBucket;
import play.libs.F;

import java.util.Collection;

public class User {
    public String id;
    public String name;
    public String email;

    public User() {}

    public User(String id, String name, String email) {
        this.id = id;
        this.name = name;
        this.email = email;
    }

    public static CouchbaseBucket bucket = Couchbase.bucket("default");

    public static F.Promise<User> findById(String id) {
        return bucket.get(id, User.class);
    }

    public static F.Promise<Collection<User>> findAll() {
        return bucket.find("users", "by_name",
                new Query().setIncludeDocs(true).setStale(Stale.FALSE), User.class);
    }

    public static F.Promise<F.Option<User>> findByName(String name) {
        Query query = new Query()
                .setLimit(1)
                .setIncludeDocs(true)
                .setStale(Stale.FALSE)
                .setRangeStart(ComplexKey.of(name))
                .setRangeEnd(ComplexKey.of(name + "\uefff"));
        return bucket.find("users", "by_name", query, User.class)
            .map(new F.Function<Collection<User>, F.Option<User>>() {
                @Override
                public F.Option<User> apply(Collection<User> users) throws Throwable {
                    if (users.isEmpty()) {
                        return F.Option.None();
                    }
                    return F.Option.Some(users.iterator().next());
                }
            });
    }

    public static F.Promise<F.Option<User>> findByEmail(String email) {
        Query query = new Query()
                .setLimit(1)
                .setIncludeDocs(true)
                .setStale(Stale.FALSE)
                .setRangeStart(ComplexKey.of(email))
                .setRangeEnd(ComplexKey.of(email + "\uefff"));
        return bucket.find("users", "by_email", query, User.class)
                .map(new F.Function<Collection<User>, F.Option<User>>() {
                    @Override
                    public F.Option<User> apply(Collection<User> users) throws Throwable {
                        if (users.isEmpty()) {
                            return F.Option.None();
                        }
                        return F.Option.Some(users.iterator().next());
                    }
                });
    }

    public static F.Promise<OperationStatus> save(User user) {
        return bucket.set(user.id, user);
    }

    public static F.Promise<OperationStatus> remove(User user) {
        return bucket.delete(user.id);
    }
}
