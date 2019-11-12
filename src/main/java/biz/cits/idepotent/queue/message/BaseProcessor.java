package biz.cits.idepotent.queue.message;

import java.util.List;

public interface BaseProcessor<T> {
    void processObservedBuffered(List<T> t);
    void processObserved(T t);
}
