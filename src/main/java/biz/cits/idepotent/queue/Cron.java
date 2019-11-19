package biz.cits.idepotent.queue;

import biz.cits.idepotent.queue.subscriber.BaseSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@EnableScheduling
public class Cron {

    private static final Logger logger = LoggerFactory.getLogger(Cron.class);

    private final BaseSubscriber baseSubscriber;

    @Autowired
    public Cron(BaseSubscriber baseSubscriber) {
        this.baseSubscriber = baseSubscriber;
    }

    @Scheduled(cron = "${queue.processor.cron}")
    public void processMessages() {
        logger.info("Processing Messages");
        baseSubscriber.processMessages();
    }

}
