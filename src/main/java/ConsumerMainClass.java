public class ConsumerMainClass {
    public static void main(String[] args) {
        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
        consumerThread.start();

    }
}
