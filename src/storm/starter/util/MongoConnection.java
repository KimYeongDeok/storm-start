package storm.starter.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class MongoConnection {
    public static void insert(String key, String val){
        DBCollection col = null;

        try {
            MongoClient mongoClient = new MongoClient("localhost");
            DB db = mongoClient.getDB("mydb");
            col = db.getCollection("test");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        assert col != null;
        col.insert(new BasicDBObject(key, val));
    }
}
