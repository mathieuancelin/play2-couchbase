package models;

public class ShortURL {
    public String id;
    public String originalUrl;
    public String t = "shorturl";

    public ShortURL() {}

    public ShortURL(String id, String originalUrl, String t) {
        this.id = id;
        this.originalUrl = originalUrl;
        this.t = t;
    }
}

