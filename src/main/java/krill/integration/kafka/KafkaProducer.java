package krill.integration.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import krill.integration.util.Utils;
import lombok.Data;

import java.io.Serializable;
import java.util.Properties;

public class KafkaProducer {

    private static final String DEFAULT_PROPERTIES = "kafka/producer-defaults.properties";

    private Producer<byte[], byte[]> producer;
    private String defaultTopic;

    public KafkaProducer(String defaultTopic, String brokerList, Properties baseProperties) {
        this.defaultTopic = defaultTopic;
        Properties config = new Properties();
        config.putAll(Utils.loadProperties(DEFAULT_PROPERTIES));
        if(baseProperties != null) config.putAll(baseProperties);
        config.put("metadata.broker.list", brokerList);

        ProducerConfig producerConfig = new ProducerConfig(config);
        this.producer = new Producer<>(producerConfig);
    }

    private KeyedMessage<byte[], byte[]> toMessage(byte[] key, byte[] value, String topic) {
        if (defaultTopic == null && topic == null) throw new IllegalArgumentException("Must provide topic or default topic");
        if (topic == null) topic = defaultTopic;
        if (key != null) return new KeyedMessage<>(topic, key, value);
        else return new KeyedMessage<>(topic, value);
    }

    public void send(byte[] value) { producer.send(toMessage(null, value, null));}

    public void send(byte[] value, String topic) {
        producer.send(toMessage(null, value, topic));
    }

    public void send(byte[] key, byte[] value, String topic) {
        producer.send(toMessage(key, value, topic));
    }

    public void shutdown() {
        if (producer != null) producer.close();
    }

    @Data
    public static class KafkaProducerFactory implements Serializable {

        String topic;
        String brokerList;
        Properties config;

        public KafkaProducerFactory(String topic, String brokerList, Properties config) {
            this.topic = topic;
            this.brokerList = brokerList;
            this.config = config;
        }

        public KafkaProducer newInstance() { return new KafkaProducer(topic, brokerList, config);}



    }
}
