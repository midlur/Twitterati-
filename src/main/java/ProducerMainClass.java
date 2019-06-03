

public class ProducerMainClass {
    public static void main(String[] args) {

        Producer producerThread = new Producer(KafkaProperties.TOPIC);
        producerThread.start();

    }
}
