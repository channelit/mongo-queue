package biz.cits.idepotent.queue.subscriber;

import biz.cits.idepotent.queue.consumer.MongoConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MsgSubscriber {

    private final MongoConsumer consumer;
    private final String MONGO_QUEUE;

    @Autowired
    public MsgSubscriber(@Value("${db.mongo.queue}") String MONGO_QUEUE, MongoConsumer consumer) {
        this.consumer = consumer;
        this.MONGO_QUEUE = MONGO_QUEUE;
    }

    public void processMessages() throws Throwable {
        consumer.subscribe(MONGO_QUEUE);
    }

}
