package krill.integration.zookeeper;

import krill.integration.exception.KrillException;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZooKeeperEmbedded {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperEmbedded.class);

    private TestingServer testingServer;
    private int port;

    public ZooKeeperEmbedded(int port) {
        log.debug("Starting embedded ZooKeeper server on port {} ...", port);
        try {
            this.port = port;
            testingServer = new TestingServer(port);
        } catch (Exception e) {
            throw new KrillException("Error starting Zookeeker server", e);
        }
        log.debug("Startup of embedded ZooKeeper server on port {} ...", port);
    }

    public void stop() {
        log.debug("Shutting down embedded ZooKeeper server on port {} ...", port);
        try {
            testingServer.close();
        } catch (IOException e) {
            throw new KrillException("Error stopping Zookeeker server", e);
        }
        log.debug("Shutdown of embedded ZooKeeper server on port {} completed", port);
    }

    public String getConnectString() {
        return testingServer.getConnectString();
    }

    public static void main(String[] args) {
        new ZooKeeperEmbedded(InstanceSpec.getRandomPort());
    }
}
