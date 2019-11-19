package biz.cits.idepotent.queue.consumer;

import biz.cits.idepotent.queue.message.BaseProcessor;
import biz.cits.idepotent.queue.subscriber.BaseSubscriber;
import biz.cits.idepotent.queue.zk.ZkNodeWatcher;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class MongoSubscriber implements BaseSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSubscriber.class);
    private final MongoDatabase mongoDatabase;
    private final String MY_ID;
    private final ZkNodeWatcher zkNodeWatcher;
    private final BaseProcessor processor;
    private final String queueName;
    private final Integer processorBuffer;

    @Autowired
    public MongoSubscriber(MongoDatabase mongoDatabase, @Value("${my.id}") String my_id, ZkNodeWatcher zkNodeWatcher, BaseProcessor baseProcessor, @Value("${db.mongo.queue}") String queueName,@Value("${queue.processor.buffer.size}") Integer processorBuffer) {
        this.mongoDatabase = mongoDatabase;
        MY_ID = my_id;
        this.zkNodeWatcher = zkNodeWatcher;
        this.processor = baseProcessor;
        this.queueName = queueName;
        this.processorBuffer = processorBuffer;
    }

    @Override
    public void processMessages() {
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        processNodePath = processNodePath.substring(processNodePath.lastIndexOf('/') + 1);
        System.out.println("process path ->" + processNodePath);
        List<Bson> updatePipeline = Collections.singletonList(
                Aggregates.match(
                        Filters.and(
                                Document.parse("{'fullDocument.status':'new'}"),
                                Document.parse("{'fullDocument.source':'" + MY_ID + "'}"),
                                Document.parse("{'fullDocument.assigned':'" + processNodePath + "'}")
                        )
                )
        );
        ChangeStreamPublisher<Document> publisher = mongoDatabase.getCollection(queueName).watch(updatePipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
        Observable<ChangeStreamDocument<Document>> observable = Observable.fromPublisher(publisher);
        LOG.info("filter set for process {}", processNodePath);
        Flowable<ChangeStreamDocument<Document>>  flowable = observable.toFlowable(BackpressureStrategy.BUFFER);
        flowable.onBackpressureBuffer(processorBuffer);
        flowable.buffer(10).subscribe(this.processor::processObservedBuffered);


        // DO NOT DELETE THIS
        //        observable.forEach(t -> {
        //            processObserved(t);
        //            LOG.trace(t.getFullDocument().toJson());
        //        });
    }

    @Override
    public boolean processorBufferOverflow() {
        return false;
    }
}
