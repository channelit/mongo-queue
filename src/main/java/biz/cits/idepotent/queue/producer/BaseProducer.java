package biz.cits.idepotent.queue.producer;

import java.util.Map;
import java.util.Optional;

public interface BaseProducer<T> {


    void sendMessage(String key, String value, Optional<Map<String, String>> assignment);
}
