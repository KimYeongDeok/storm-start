package storm.starter;

import com.mongodb.*;
import org.testng.annotations.Test;

import java.net.UnknownHostException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class TestMongoDBConnection {
    @Test
    public void test(){
        DBCollection col = null;

          try {
              MongoClient mongoClient = new MongoClient("localhost");
              DB db = mongoClient.getDB("mydb");
              col = db.getCollection("test");
          } catch (UnknownHostException e) {
              e.printStackTrace();
          }

//        assert col != null;
        BasicDBObject doc = new BasicDBObject("name", "MongoDB");
        col.insert(doc);

    }

}
