package controllers;

import models.ShortURL;
import org.ancelin.play2.java.couchbase.Couchbase;
import org.ancelin.play2.java.couchbase.crud.CrudSource;
import org.ancelin.play2.java.couchbase.crud.CrudSourceController;

public class ShortURLController extends CrudSourceController<ShortURL> {

    private final CrudSource<ShortURL> source = new CrudSource<ShortURL>( Couchbase.bucket("default"), ShortURL.class );

    @Override
    public CrudSource<ShortURL> getSource() {
        return source;
    }

    @Override
    public String defaultDesignDocname() {
        return "shorturls";
    }

    @Override
    public String defaultViewName() {
        return "by_url";
    }
}
