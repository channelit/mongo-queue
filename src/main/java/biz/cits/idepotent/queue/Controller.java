package biz.cits.idepotent.queue;

import biz.cits.idepotent.queue.consumer.MongoConsumer;
import biz.cits.idepotent.queue.message.MsgGenerator;
import biz.cits.idepotent.queue.producer.MasterProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Map;

@RestController
public class Controller {


    private MasterProducer masterProducer;
    private MongoConsumer mongoConsumer;

    @Autowired
    public Controller(MasterProducer masterProducer, MongoConsumer mongoConsumer) {
        this.masterProducer = masterProducer;
        this.mongoConsumer = mongoConsumer;
    }

    @GetMapping(path = "send", produces = "application/json")
    public String sendMessages(@RequestParam int numMessage) {
        ArrayList<Map.Entry<String, String>> messages = MsgGenerator.getMessages(numMessage);
        messages.forEach((e) -> masterProducer.sendMessage(e.getKey(), e.getValue()));
        return "done";
    }

    @GetMapping(path = "recv", produces = "application/json")
    public String recvMessages() {
        mongoConsumer.processDocuments();
        return "done";
    }
}
