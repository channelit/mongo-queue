package biz.cits.idepotent.queue.worker;

import biz.cits.idepotent.queue.db.DataStore;
import biz.cits.idepotent.queue.message.MsgGenerator;
import biz.cits.idepotent.queue.producer.BaseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class MasterProducer implements BaseProducer<String> {

    private final DataStore dataStore;

    private static final Logger LOG = LoggerFactory.getLogger(MasterProducer.class);

    @Autowired
    public MasterProducer(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void sendMessage(String key, String value, Optional<Map<String, String>> data) {
        HashMap<String, String> records = new HashMap<>();
        records.put("data", value);
        data.ifPresent(records::putAll);
        dataStore.queueData(key, records);
    }

    @Override
    public void generateSendMessages(Integer producerBatchSize, Optional<Map<String, String>> assignment, int cnt) {
        ArrayList<Map.Entry<String, String>> messages = MsgGenerator.getMessages(producerBatchSize);
        this.sendMessage(messages, assignment);
        messages.forEach((e) -> this.sendMessage(e.getKey(), e.getValue(), assignment));
    }

    private void sendMessage(ArrayList<Map.Entry<String, String>> messages, Optional<Map<String, String>> assignment) {

    }

}
