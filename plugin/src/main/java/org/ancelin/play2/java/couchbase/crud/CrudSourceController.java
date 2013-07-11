package org.ancelin.play2.java.couchbase.crud;

import play.mvc.Controller;
import play.mvc.Result;

public abstract class CrudSourceController<T> extends Controller {

    public abstract CrudSource<T> getSource();

    private String path    = "";
    private String Slash   = "/?";
    private String Id      = "/([^/]+)/?";
    private String Partial = "/([^/]+)/partial";
    private String Find    = "/find/?";
    private String Batch   = "/batch/?";
    private String Stream  = "/stream/?";

    private String extractId(String path) {
        return path.replace("/", "");
    }

    public Result onRequest() {
        if (request().path().startsWith(path)) {
            String method = request().method();
            String partialPath = request().path().substring(0, path.length());
            if (method.equals("GET") && partialPath.matches(Id)) {
                return get(extractId(partialPath));
            } else if (method.equals("GET") && partialPath.matches(Slash)) {
                return find();
            } else if (method.equals("PUT") && partialPath.matches(Partial)) {
                return updatePartial(extractId(partialPath));
            } else if (method.equals("PUT") && partialPath.matches(Id)) {
                return update(extractId(partialPath));
            } else if (method.equals("PUT") && partialPath.matches(Batch)) {
                return batchUpdate();
            } else if (method.equals("POST") && partialPath.matches(Batch)) {
                return batchInsert();
            } else if (method.equals("POST") && partialPath.matches(Find)) {
                return find();
            } else if (method.equals("POST") && partialPath.matches(Slash)) {
                return insert();
            } else if (method.equals("DELETE") && partialPath.matches(Batch)) {
                return batchDelete();
            } else if (method.equals("DELETE") && partialPath.matches(Id)) {
                return delete(extractId(partialPath));
            } else {
                return notFound();
            }
        } else {
            return notFound();
        }
    }

    public Result get(String id) {
        return null;
    }

    public Result insert() {
        return null;
    }

    public Result delete(String id) {
        return null;
    }

    public Result update(String id) {
        return null;
    }

    public Result updatePartial(String id) {
        return null;
    }

    public Result batchInsert() {
        return null;
    }

    public Result batchDelete() {
        return null;
    }

    public Result batchUpdate() {
        return null;
    }

    public Result find() {
        return null;
    }
}
