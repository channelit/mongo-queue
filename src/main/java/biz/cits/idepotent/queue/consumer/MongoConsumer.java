package biz.cits.idepotent.queue.consumer;

import biz.cits.idepotent.queue.message.BaseProcessor;
import biz.cits.idepotent.queue.zk.ZkNodeWatcher;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoDatabase;
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
public class MongoConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MongoConsumer.class);
    private final MongoDatabase mongoDatabase;
    private final String MY_ID;
    private final ZkNodeWatcher zkNodeWatcher;
    private final BaseProcessor processor;

    @Autowired
    public MongoConsumer(MongoDatabase mongoDatabase, @Value("${my.id}") String my_id, ZkNodeWatcher zkNodeWatcher, BaseProcessor baseProcessor) {
        this.mongoDatabase = mongoDatabase;
        MY_ID = my_id;
        this.zkNodeWatcher = zkNodeWatcher;
        this.processor = baseProcessor;
    }

    public void subscribe(String collection) {
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
        ChangeStreamPublisher<Document> publisher = mongoDatabase.getCollection(collection).watch(updatePipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
        Observable<ChangeStreamDocument<Document>> observable = Observable.fromPublisher(publisher);
        observable.buffer(10).subscribe(this.processor::processObservedBuffered);

        // DO NOT DELETE THIS
        //        observable.forEach(t -> {
        //            processObserved(t);
        //            LOG.trace(t.getFullDocument().toJson());
        //        });
    }

}
