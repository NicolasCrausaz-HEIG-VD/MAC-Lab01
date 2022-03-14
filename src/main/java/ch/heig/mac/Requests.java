package ch.heig.mac;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
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
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT imdb.id as imdb_id, tomatoes.viewer.rating as tomatoes_rating, imdb.rating as imdb_rating\n" +
                        "FROM movies\n" +
                        "WHERE tomatoes.viewer.rating > 0 AND ABS(imdb.rating - tomatoes.viewer.rating) > 7;"
        );
        return result.rowsAsObject();
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
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT RAW u.name\n" +
                        "FROM users u\n" +
                        "INNER JOIN comments comment ON u.email = comment.email\n" +
                        "GROUP BY u.name\n" +
                        "HAVING COUNT(comment) > 300;"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT DISTINCT imdb.id as imdb_id, imdb.rating, `cast`\n" +
                        "FROM movies\n" +
                        "WHERE ($actor WITHIN `cast`) AND imdb.rating <> '' AND imdb.rating > 8;"
                ,
                QueryOptions.queryOptions().parameters(JsonObject.create().put("actor", actor))
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> plentifulDirectors() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT d AS director_name, COUNT(*) as count_film\n" +
                        "FROM movies\n" +
                        "UNNEST directors d\n" +
                        "GROUP BY d\n" +
                        "HAVING COUNT(*) > 30;"
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> confusingMovies() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT movies.imdb.id as movie_id, movies.title\n" +
                        "FROM movies\n" +
                        "WHERE ARRAY_LENGTH(movies.directors) > 20"
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT c.movie_id, CONCAT2(\", \",ARRAY_AGG(c.text)) as text\n" +
                        "FROM movies m\n" +
                        "INNER JOIN comments c ON m._id = c.movie_id\n" +
                        "WHERE $director WITHIN m.directors\n" +
                        "GROUP BY c.movie_id",
                QueryOptions.queryOptions().parameters(JsonObject.create().put("director", director))
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT movie_id,\n" +
                        "       CONCAT2(\", \",ARRAY_AGG(c.text)) AS text\n" +
                        "FROM comments c\n" +
                        "WHERE c.movie_id IN (\n" +
                        "    SELECT RAW m._id\n" +
                        "    FROM movies m\n" +
                        "    WHERE $director WITHIN m.directors)\n" +
                        "GROUP BY movie_id",
                        QueryOptions.queryOptions().parameters(JsonObject.create().put("director", director))
        );
        return result.rowsAsObject();
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "UPDATE theaters t\n" +
                        "SET t.schedule = ARRAY v FOR v IN t.schedule WHEN v.hourBegin >= \"18:00:00\" END\n" +
                        "WHERE ANY v IN t.schedule SATISFIES v.movieId = $id AND v.hourBegin < \"18:00:00\" END\n" +
                        "RETURNING t;",
                QueryOptions.queryOptions().parameters(JsonObject.create().put("id", movieId))
        );

        return result.rowsAsObject().size();
    }

    public List<JsonObject> nightMovies() {
        QueryResult result = cluster.bucket("mflix-sample").defaultScope().query(
                "SELECT filmsWithSchedules.movieId movie_id, m.title AS title\n" +
                        "FROM (\n" +
                        "      SELECT sch.movieId, ARRAY_AGG(sch.hourBegin) AS filmScheds\n" +
                        "      FROM theaters t\n" +
                        "      UNNEST t.schedule AS sch\n" +
                        "      GROUP BY sch.movieId\n" +
                        ") AS filmsWithSchedules\n" +
                        "JOIN movies m ON m._id = filmsWithSchedules.movieId\n" +
                        "WHERE EVERY startTime IN filmsWithSchedules.filmScheds SATISFIES startTime >= \"18:00:00\" END;");

        return result.rowsAsObject();
    }
}
