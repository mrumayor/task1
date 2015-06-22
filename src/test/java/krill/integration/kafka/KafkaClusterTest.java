package krill.integration.kafka;

import kafka.server.KafkaServer;
import krill.integration.zookeeper.ZooKeeperEmbedded;
import org.apache.curator.test.InstanceSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

public class KafkaClusterTest {

    private KafkaCluster kafkaCluster;
    private ZooKeeperEmbedded zooKeeperEmbedded;

    private KafkaProducer kafkaProducer;
    private KafkaHighLevelConsumer consumer;
    private KafkaSimpleConsumer simpleConsumer;

    private static final String TOPIC_NAME = "kafkaTopic";
    private static final int NUM_PARTITIONS = 3;


    @Before
    public void setUp() {
        zooKeeperEmbedded = new ZooKeeperEmbedded(InstanceSpec.getRandomPort());
        kafkaCluster = new KafkaCluster(3, zooKeeperEmbedded.getConnectString());
        kafkaCluster.start();
        kafkaCluster.createTopic(TOPIC_NAME, NUM_PARTITIONS, 1, new Properties());
    }


    @Test
    public void testThatProduceAndConsumeWorks() {
        int NUM_MESSAGES = 100;
        int NUM_THREADS = 3;

        System.out.println(kafkaCluster.getBrokerList());
        // Create consumer
        consumer = new KafkaHighLevelConsumer(TOPIC_NAME, "0", zooKeeperEmbedded.getConnectString(), NUM_THREADS, new Properties());
        consumer.startConsumers(NUM_THREADS);

        simpleConsumer = new KafkaSimpleConsumer();

        System.out.println(zooKeeperEmbedded.getConnectString());
        List<String> seeds = new ArrayList<String>();
        seeds.add("localhost");
        try {
          //  simpleConsumer.run(10, TOPIC_NAME, NUM_PARTITIONS, seeds, 60012);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }


        // Create The producer Factory
        kafkaProducer = new KafkaProducer(TOPIC_NAME, kafkaCluster.getBrokerList(), new Properties());

        String message = new String("hola");
        byte[] messageBytes = message.getBytes();


        Random rnd = new Random();

        for (int i=0; i < NUM_MESSAGES; i++) {
            byte[] key = ByteBuffer.allocate(4).putInt(rnd.nextInt(255)).array();
            kafkaProducer.send(key,messageBytes, TOPIC_NAME);
        }

        // Send messages
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<byte[]> retrievedMessages = consumer.getMessages();
        Assert.assertEquals(NUM_MESSAGES ,retrievedMessages.size());
    }
}