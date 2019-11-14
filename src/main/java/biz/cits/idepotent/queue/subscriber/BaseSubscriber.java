package biz.cits.idepotent.queue.subscriber;

public interface BaseSubscriber {
    void processMessages();

    boolean processorBufferOverflow();
}
