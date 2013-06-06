package controllers;

import play.libs.F;
import play.mvc.*;

import models.*;

public class Application extends Controller {

    public static Result index() {
        return ok(views.html.index.render());
    }

    public static Result goTo(String id) {
        return async(
            ShortURL.findById(id).map(new F.Function<ShortURL, Result>() {
                @Override
                public Result apply(ShortURL shortURL) throws Throwable {
                    if (shortURL == null) {
                        return notFound();
                    }
                    return redirect(shortURL.originalUrl);
                }
            })
        );
    }
}