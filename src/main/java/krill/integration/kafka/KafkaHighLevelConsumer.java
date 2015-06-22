package krill.integration.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import krill.integration.util.Utils;
import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaHighLevelConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaHighLevelConsumer.class);

    private static final String DEFAULT_PROPERTIES = "kafka/consumer-defaults.properties";

    private final String topic;
    private final List<StreamConsumer> consumerThreads;
    private ExecutorService executor;
    private ConsumerConnector consumerConnector;

    public KafkaHighLevelConsumer(String topic, String group, String zookeeperConnect, int numThreads) {
        this(topic, group, zookeeperConnect, numThreads, null);
    }

    public KafkaHighLevelConsumer(String topic, String group, String zookeeperConnect, int numThreads,
                                  Properties baseProperties) {
        this.topic = topic;
        Properties config = new Properties();
        config.putAll(Utils.loadProperties(DEFAULT_PROPERTIES));
        if (baseProperties != null)
            config.putAll(baseProperties);
        config.put("zookeeper.connect", zookeeperConnect);
        config.put("group.id", group);

        this.consumerThreads = new Vector<>();
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(config));

        log.debug("Connecting to topic {} via ZooKeeper {}", topic, zookeeperConnect);
    }

    public void startConsumers(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, numThreads);
        Decoder defaultDecoder = new DefaultDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams
                (topicCountMap, defaultDecoder, defaultDecoder);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // Now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            StreamConsumer consumerThread = new StreamConsumer(stream, new StreamConsumerContext(threadNumber));
            executor.submit(consumerThread);
            consumerThreads.add(consumerThread);
            threadNumber++;
        }

        executor.shutdown();
    }

    public List<byte[]> getMessages() {
        List<byte[]> messages = new ArrayList<>();
        for (StreamConsumer consumerThread : consumerThreads) {
            messages.addAll(consumerThread.getMessages());
        }
        return messages;
    }

    public void shutdown() {
        if (consumerConnector != null) consumerConnector.shutdown();
        if (executor != null) executor.shutdown();
    }

    /**
     * A consumer thread to read Kafka messages
     */
    private class StreamConsumer implements Runnable {

        private final KafkaStream stream;
        private final StreamConsumerContext context;

        @Getter
        private List<byte[]> messages;

        public StreamConsumer(KafkaStream stream, StreamConsumerContext context) {
            this.stream = stream;
            this.context = context;
            messages = new ArrayList<>();
        }

        @Override
        public void run() {
            ConsumerIterator it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> data = it.next();
                log.debug("Received message in topic:{}, partition:{}", data.topic(), data.partition());
                log.debug("Received messaged {} by thread:{}, topic:{}, partition:{}, offset:{} ", data.message()
                        .toString(), context.getThreadId());
                messages.add(data.message());
            }
            log.debug("Shutting down Kafka consumer Thread:{}", context.getThreadId());

            // TODO graceful shutdown
        }

        public List<byte[]> getMessages() {
            return messages;
        }
    }

    /**
     * StreamConsumer context
     */
    @Data
    private class StreamConsumerContext {
        private final int threadId;

        public StreamConsumerContext(int threadId) {
            this.threadId = threadId;
        }

        public int getThreadId() {
            return threadId;
        }
    }
}
