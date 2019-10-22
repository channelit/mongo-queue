package biz.cits.idepotent.queue.producer;

import biz.cits.idepotent.queue.db.DataStore;
import biz.cits.idepotent.queue.zk.ZkNodeWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Component
public class MasterProducer {

    @Autowired
    private DataStore dataStore;

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterProducer.class);

    public void sendMessage(String key, String value) {
        LOGGER.info("Sending message " + value);
        HashMap<String, String> records = new HashMap<>();
        records.put(key, value);
        dataStore.queueData(key, records);
    }
}
