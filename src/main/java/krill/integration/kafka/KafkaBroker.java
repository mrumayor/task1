package krill.integration.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
//import krill.integration.exception.KrillException;
//import Utils;
import krill.integration.exception.KrillException;
import krill.integration.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.SecureRandom;
import java.util.Properties;

public class KafkaBroker {

    private static Logger log = LoggerFactory.getLogger(KafkaBroker.class);

    private static final String DEFAULT_PROPERTIES = "kafka/broker-defaults.properties";

    private static final String DEFAULT_ZKCONNECT = "127.0.0.1:2181";
    private static final String TEMP_DIR_PREFIX = "kafka-test";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Properties config;
    private KafkaServerStartable kafkaServer;

    public KafkaBroker(int port, String zooKeeperConnect, int brokerId) {
        this.config = new Properties();
        //String SPECIFIC_PROPERTIES = "kafka/broker"+ Integer.toString(brokerId)  +".properties";

        this.config.putAll(Utils.loadProperties(DEFAULT_PROPERTIES));
        this.config.put("broker.id", Integer.toString(brokerId));
        this.config.put("port", Integer.toString(port));
        this.config.put("log.dirs", getTempDir().getAbsolutePath());
        this.config.put("zookeeper.connect", zooKeeperConnect);

        KafkaConfig kafkaConfig = new KafkaConfig(config);
        this.kafkaServer = new KafkaServerStartable(kafkaConfig);
    }

    private File getTempDir() {
        File file = new File(System.getProperty("java.io.tmpdir"), TEMP_DIR_PREFIX + "logs-" + RANDOM.nextInt
                (10000000));
        if (!file.mkdirs()) {
            throw new KrillException("Could not create temp directory: " + file.getAbsolutePath());
        }
        file.deleteOnExit();
        return file;
    }

    public String getZooKeeperConnect() {
        String zkConnectLookup = config.getProperty("zookeeper.connect");
        if (zkConnectLookup != null) return zkConnectLookup;
        else {
            log.warn("Zookeeper.connect is not configured -- falling back to default setting " + DEFAULT_ZKCONNECT);
            return DEFAULT_ZKCONNECT;
        }
    }

    public java.util.Properties getConfig() {
        return config;
    }
    public String getBrokerList() {
        return kafkaServer.serverConfig().hostName() + ":" + kafkaServer.serverConfig().port();
    }

    public void start() {
        log.debug("Starting embedded Kafka broker at {} (with ZK server at {}) ...", getBrokerList(),
                getZooKeeperConnect());
        kafkaServer.startup();
        log.debug("Startup of embedded Kafka broker at {} (with ZK server at {}) ...", getBrokerList(),
                getZooKeeperConnect());
    }

    public void stop() {
        log.debug("Shutting down embedded Kafka broker at {} (with ZK server at {}) ...", getBrokerList(),
                getZooKeeperConnect());
        if (kafkaServer != null) kafkaServer.shutdown();
        log.debug("Shutdown of embedded Kafka broker at {} (with ZK server at {}) ...", getBrokerList(),
                getZooKeeperConnect());
    }


}
