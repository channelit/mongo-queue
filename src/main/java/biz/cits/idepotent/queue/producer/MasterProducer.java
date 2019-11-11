package biz.cits.idepotent.queue.producer;

import biz.cits.idepotent.queue.db.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class MasterProducer {

    private final DataStore dataStore;

    private static final Logger LOG = LoggerFactory.getLogger(MasterProducer.class);

    @Autowired
    public MasterProducer(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void sendMessage(String key, String value, Optional<Map<String, String>> data) {
        HashMap<String, String> records = new HashMap<>();
        records.put("data", value);
        data.ifPresent(records::putAll);
        dataStore.queueData(key, records);
    }
}
