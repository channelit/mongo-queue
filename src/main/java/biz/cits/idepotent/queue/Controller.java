package biz.cits.idepotent.queue;

import biz.cits.idepotent.queue.message.MsgGenerator;
import biz.cits.idepotent.queue.producer.Master;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Map;

@RestController
public class Controller {

    private Master master;

    @Autowired
    public Controller(Master master) {
        this.master = master;
    }

    @GetMapping(path = "send", produces = "application/json")
    public String sendMessages(@RequestParam int numMessage) {
        ArrayList<Map.Entry<String, String>> messages = MsgGenerator.getMessages(numMessage);
        messages.forEach((e) -> master.sendMessage(e.getKey(), e.getValue()));
        return "done";
    }
}
