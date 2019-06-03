import com.mongodb.DB;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

public class ConsumerMainClass {
    public static void main(String[] args) {

        try {
            MongoClient mongo =new MongoClient("localhost" , 27017);
            DB database = mongo.getDB("twitterati");
            database.createCollection("tweetDB",null);
            Consumer consumerThread = new Consumer(KafkaProperties.TOPIC,database);
            consumerThread.start();
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }


    }
}
