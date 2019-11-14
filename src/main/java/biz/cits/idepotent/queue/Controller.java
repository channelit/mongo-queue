package biz.cits.idepotent.queue;

import biz.cits.idepotent.queue.message.MsgGenerator;
import biz.cits.idepotent.queue.subscriber.BaseSubscriber;
import biz.cits.idepotent.queue.worker.MasterProducer;
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
import java.util.Optional;

@RestController
public class Controller {

    private final MasterProducer masterProducer;
    private final MongoDatabase mongoDatabase;
    private final BaseSubscriber baseSubscriber;

    @Autowired
    public Controller(MasterProducer masterProducer, MongoDatabase mongoDatabase, BaseSubscriber baseSubscriber) {
        this.masterProducer = masterProducer;
        this.mongoDatabase = mongoDatabase;
        this.baseSubscriber = baseSubscriber;
    }

    @GetMapping(path = "send", produces = "application/json")
    public String sendMessages(@RequestParam int numMessage) {
        ArrayList<Map.Entry<String, String>> messages = MsgGenerator.getMessages(numMessage);
        messages.forEach((e) -> masterProducer.sendMessage(e.getKey(), e.getValue(), Optional.empty()));
        return "done";
    }

    @GetMapping(path = "recv", produces = "application/json")
    public String recvMessages() throws Throwable {
        baseSubscriber.processMessages();
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
