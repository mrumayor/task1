package krill.integration.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import lombok.Data;
import org.I0Itec.zkclient.ZkClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public @Data
class KafkaCluster {

    private static final int[] PORTS = {60011, 60012, 60013, 60014, 60015, 60016, 60017};

    private final List<KafkaBroker> kafkaServers;
    private final List<KafkaTopic> topics;

    private final String zooKeeperConnect;

    public KafkaCluster(int numKafkaServers, String zooKeeperConnect) {
        this.kafkaServers = new ArrayList();
        this.topics = new ArrayList();
        this.zooKeeperConnect = zooKeeperConnect;

        for (int i = 0; i < numKafkaServers; i++) {
            KafkaBroker kafkaEmbedded = new KafkaBroker(PORTS[i], zooKeeperConnect, i);
            this.kafkaServers.add(kafkaEmbedded);
        }
    }
    // Se que esto se puede hace con lombok pero no se porque no puedo generarlos
    public List<KafkaBroker> getKafkaServers()
    {
        return kafkaServers;
    }

    public void createTopic(String topic, int numPartitions, int replicationFactor, Properties baseProperties) {
        int sessionTimeout = 30000;
        int connectionTimeout = 30000;

        ZkClient zkClient = new ZkClient(zooKeeperConnect, sessionTimeout, connectionTimeout,
                ZKStringSerializer$.MODULE$);
        AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, baseProperties);
        topics.add(new KafkaTopic(topic, numPartitions, replicationFactor, baseProperties));
        zkClient.close();
    }

    public String getBrokerList() {
        StringBuilder brokerList = new StringBuilder();
        for (KafkaBroker kafkaServer : kafkaServers) {
            brokerList.append(kafkaServer.getBrokerList()).append(",");
        }
        return brokerList.substring(0, brokerList.length() - 1);
    }

    public void start() {
        for (KafkaBroker kafkaServer : kafkaServers) {
            kafkaServer.start();
        }
    }

    public void stop() {
        kafkaServers.stream().filter(kafkaServer -> kafkaServer != null).forEach(KafkaBroker::stop);
    }


}
