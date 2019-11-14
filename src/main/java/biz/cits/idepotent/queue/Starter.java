package biz.cits.idepotent.queue;

import biz.cits.idepotent.queue.subscriber.BaseSubscriber;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(3)
public class Starter implements ApplicationRunner {
    private final Boolean processorStart;
    private final BaseSubscriber baseSubscriber;

    @Autowired
    public Starter(@Value("${queue.processor.start}") Boolean processorStart, BaseSubscriber baseSubscriber) {
        this.processorStart = processorStart;
        this.baseSubscriber = baseSubscriber;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (processorStart) {
            baseSubscriber.processMessages();
        }
    }
}
