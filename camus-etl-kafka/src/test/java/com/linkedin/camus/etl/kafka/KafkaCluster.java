package com.linkedin.camus.etl.kafka;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.Utils;


public class KafkaCluster {

    private static final Random RANDOM = new Random();
    private static final String TEMP_DIR_PREFIX = "camus-";

    private final EmbeddedZookeeper zookeeper;
    private final List<KafkaServer> brokers;
    private final Properties props;

    public KafkaCluster() throws IOException {
        this(new Properties());
    }

    public KafkaCluster(Properties props) throws IOException {
        this(props, 1);
    }

    public KafkaCluster(Properties baseProperties, int numOfBrokers) throws IOException {

        zookeeper = new EmbeddedZookeeper();
        brokers = new ArrayList<>();

        props = new Properties();

        props.putAll(baseProperties);

        StringBuilder builder = null;

        for (int i = 0; i < numOfBrokers; ++i) {
            if (builder != null) {
                builder.append(",");
            } else {
                builder = new StringBuilder();
            }

            int brokerPort = getAvailablePort();

            builder.append("localhost:");
            builder.append(brokerPort);

            Properties properties = new Properties();
            properties.putAll(baseProperties);
            properties.setProperty("zookeeper.connect", zookeeper.getConnection());
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", "localhost");
            properties.setProperty("port", Integer.toString(brokerPort));
            properties.setProperty("log.dir", getTempDir().getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));

            brokers.add(startBroker(properties));
        }

        props.put("metadata.broker.list", builder.toString());
        props.put("zookeeper.connect", zookeeper.getConnection());

    }

    private static KafkaServer startBroker(Properties props) {
        KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime());
        server.startup();
        return server;
    }

    private static File getTempDir() {
        File file = new File(System.getProperty("java.io.tmpdir"),
                             TEMP_DIR_PREFIX + RANDOM.nextInt(10000000));
        if (!file.mkdirs()) {
            throw new RuntimeException(
                    "could not create temp directory: " + file.getAbsolutePath());
        }
        file.deleteOnExit();
        return file;
    }

    private static int getAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public Properties getProps() {
        Properties props = new Properties();
        props.putAll(this.props);
        return props;
    }

    public void shutdown() {
        for (KafkaServer broker : brokers) {
            broker.shutdown();
        }
        zookeeper.shutdown();
    }

    public static class SystemTime implements Time {

        public long milliseconds() {
            return System.currentTimeMillis();
        }

        public long nanoseconds() {
            return System.nanoTime();
        }

        public void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    private static class EmbeddedZookeeper {

        private final int port;
        private final File snapshotDir;
        private final File logDir;
        private final NIOServerCnxn.Factory factory;

        /**
         * Constructs an embedded Zookeeper instance.
         *
         * @throws IOException if an error occurs during Zookeeper initialization.
         */
        public EmbeddedZookeeper() throws IOException {
            port = getAvailablePort();
            snapshotDir = getTempDir();
            logDir = getTempDir();
            factory = new NIOServerCnxn.Factory(new InetSocketAddress("localhost", port),
                                                     1024);

            try {
                int tickTime = 500;
                factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        /**
         * Shuts down the embedded Zookeeper instance.
         */
        public void shutdown() {
            factory.shutdown();
            Utils.rm(snapshotDir);
            Utils.rm(logDir);
        }

        public String getConnection() {
            return "localhost:" + port;
        }

    }

}
