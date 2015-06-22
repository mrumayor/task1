package krill.integration.kafka;

import lombok.Data;

import java.util.Properties;

@Data
public class KafkaTopic {

    private String name;
    private int numPartitions;
    private int replicationFactor;
    private Properties config;

    public KafkaTopic(String name, int numPartitions, int replicationFactor, Properties config) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.config = config;
    }
}