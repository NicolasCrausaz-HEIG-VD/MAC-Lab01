package ch.heig.mac;

import java.util.List;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;


public class Requests {
    private final Cluster cluster;

    public Requests(Cluster cluster) {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames() {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT imdb.id as imdb_id, tomatoes.viewer.rating as tomatoes_rating, imdb.rating as imdb_rating\n" +
                        "FROM movies\n" +
                        "WHERE tomatoes.viewer.rating > 0 AND ABS(imdb.rating - tomatoes.viewer.rating) > 7;"
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> hiddenGem() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> topReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<String> greatReviewers() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT RAW u.name\n" +
                        "FROM users u\n" +
                        "INNER JOIN comments comment On u.email = comment.email\n" +
                        "GROUP BY u.name\n" +
                        " HAVING COUNT(comment) > 300;"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT DISTINCT imdb.id as imdb_id, imdb.rating, `cast`\n" +
                        "FROM movies\n" +
                        "WHERE ($actor WITHIN `cast`) AND imdb.rating <> \"\" AND imdb.rating > 8;"
                ,
                QueryOptions.queryOptions().parameters(JsonObject.create().put("actor", actor))
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> plentifulDirectors() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> confusingMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
