package ch.heig.mac;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

import java.util.List;


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
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> hiddenGem() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT title\n" +
                        "FROM movies\n" +
                        "WHERE tomatoes.critic.rating = 10\n" +
                        "AND tomatoes.viewer IS MISSING");
        return result.rowsAsObject();
    }

    public List<JsonObject> topReviewers() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT u.name, COUNT(u.name) as cnt\n" +
                        "FROM users u\n" +
                        "INNER JOIN comments comment ON u.email = comment.email\n" +
                        "GROUP BY u.name\n" +
                        "ORDER BY COUNT(u.name) DESC"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<String> greatReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        throw new UnsupportedOperationException("Not implemented, yet");
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
