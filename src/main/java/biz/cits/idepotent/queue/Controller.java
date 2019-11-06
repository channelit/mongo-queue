package biz.cits.idepotent.queue;

import biz.cits.idepotent.queue.consumer.MongoConsumer;
import biz.cits.idepotent.queue.db.DataStore;
import biz.cits.idepotent.queue.message.MsgGenerator;
import biz.cits.idepotent.queue.producer.MasterProducer;
import biz.cits.idepotent.queue.subscriber.MsgSubscriber;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.Observable;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Map;

@RestController
public class Controller {

    private final MasterProducer masterProducer;
    private final MongoConsumer mongoConsumer;
    private final MongoDatabase mongoDatabase;
    private final MsgSubscriber msgSubscriber;


    @Autowired
    public Controller(MasterProducer masterProducer, MongoConsumer mongoConsumer, DataStore dataStore, MongoDatabase mongoDatabase, MsgSubscriber msgSubscriber) {
        this.masterProducer = masterProducer;
        this.mongoConsumer = mongoConsumer;
        this.mongoDatabase = mongoDatabase;
        this.msgSubscriber = msgSubscriber;
    }

    @GetMapping(path = "send", produces = "application/json")
    public String sendMessages(@RequestParam int numMessage) {
        ArrayList<Map.Entry<String, String>> messages = MsgGenerator.getMessages(numMessage);
        messages.forEach((e) -> masterProducer.sendMessage(e.getKey(), e.getValue()));
        return "done";
    }

    @GetMapping(path = "recv", produces = "application/json")
    public String recvMessages() throws Throwable {
        msgSubscriber.processMessages();
        return "done";
    }

    @GetMapping(path = "find", produces = "application/json")
    public String findMessages(@RequestParam String col) {
        Publisher<Document> publisher = mongoDatabase.getCollection(col).find().first();
        Observable<Document> observable = Observable.fromPublisher(publisher);
        Document result = observable.blockingFirst();
        return result.toJson();
    }
}
