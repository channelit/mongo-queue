package biz.cits.idepotent.queue.zk;

import biz.cits.idepotent.queue.producer.BaseProducer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
public class ZkNodeWatcher implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ZkNodeWatcher.class);

    private final String ZK_ZNODE_FOLDER;
    private final BaseProducer<String> producer;
    private static final String PROCESS_NODE_PREFIX = "/p_";
    private final int id;
    private final ZkService zooKeeperService;
    private String processNodePath;
    private String watchedNodePath;
    private final Integer producerBatchSize;
    private final Integer producerPeriod;
    private final Integer producerDelay;

    @Autowired
    public ZkNodeWatcher(@Value("${queue.producer.period}") Integer producerPeriod, @Value("${queue.producer.delay}") Integer producerDelay, @Value("${zk.znode.folder}") String zk_znode_folder, BaseProducer<String> producer, @Value("${my.id}") final int id, @Value(("${zk.connect.url}")) final String zkURL, @Value("${queue.producer.batch.size}") Integer producerBatchSize) throws IOException {
        this.ZK_ZNODE_FOLDER = zk_znode_folder;
        this.producer = producer;
        this.id = id;
        this.zooKeeperService = new ZkService(zkURL, new ProcessNodeWatcher());
        this.producerBatchSize = producerBatchSize;
        this.producerPeriod = producerPeriod;
        this.producerDelay = producerDelay;
    }

    public String getProcessNodePath() {
        return processNodePath;
    }

    private void attemptForLeaderPosition() {
        final List<String> childNodePaths = zooKeeperService.getChildren(ZK_ZNODE_FOLDER, false);
        Collections.sort(childNodePaths);
        int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
        if (index == 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("[Process: " + id + "] I am the new leader!");
            }
            TimerTask tt = new TimerTask() {
                @Override
                public void run() {
                    List<String> availableChildren = zooKeeperService.getChildren(ZK_ZNODE_FOLDER, false);
                    LOG.info("Available children :  {} ", availableChildren.toString());
                    Optional<Map<String, String>> assignment = Optional.of(Collections.singletonMap("assigned", availableChildren.get(new Random().nextInt(availableChildren.size()))));
                    producer.generateSendMessages(producerBatchSize, assignment);
                }
            };
            Timer t = new Timer("Message Sender");
            t.scheduleAtFixedRate(tt, producerDelay, producerPeriod);
        } else {
            final String watchedNodeShortPath = childNodePaths.get(index - 1);
            watchedNodePath = ZK_ZNODE_FOLDER + "/" + watchedNodeShortPath;
            if (LOG.isInfoEnabled()) {
                LOG.info("[Process: " + id + "] - Setting watch on node with path: " + watchedNodePath);
            }
            zooKeeperService.watchNode(watchedNodePath, true);
        }
    }

    @Override
    public void run(ApplicationArguments args) {

        if (LOG.isInfoEnabled()) {
            LOG.info("Process with id: " + id + " has started!");
        }

        final String rootNodePath = zooKeeperService.createNode(ZK_ZNODE_FOLDER, false, false);
        if (rootNodePath == null) {
            throw new IllegalStateException("Unable to create/access leader election root node with path: " + ZK_ZNODE_FOLDER);
        }

        processNodePath = zooKeeperService.createNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
        if (processNodePath == null) {
            throw new IllegalStateException("Unable to create/access process node with path: " + ZK_ZNODE_FOLDER);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("[Process: " + id + "] Process node created with path: " + processNodePath);
        }

        attemptForLeaderPosition();
    }

    @Component
    public class ProcessNodeWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[Process: " + id + "] Event received: " + event);
            }

            final EventType eventType = event.getType();
            if (EventType.NodeDeleted.equals(eventType)) {
                if (event.getPath().equalsIgnoreCase(watchedNodePath)) {
                    attemptForLeaderPosition();
                }
            }

        }

    }

}