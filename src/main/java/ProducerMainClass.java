import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

public class ProducerMainClass {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Twitter twitter = new TwitterFactory().getInstance();
        twitter.setOAuthConsumer("API KEY","API SECRET KEY");
        twitter.setOAuthAccessToken(new AccessToken("ACCESS TOKEN","ACCESS TOKEN SECRET"));

        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync,twitter);
        producerThread.start();

    }
}
