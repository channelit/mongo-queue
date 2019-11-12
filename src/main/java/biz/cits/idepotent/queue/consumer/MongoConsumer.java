package biz.cits.idepotent.queue.consumer;

import biz.cits.idepotent.queue.zk.ZkNodeWatcher;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.Observable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MongoConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MongoConsumer.class);

    private final MongoDatabase mongoDatabase;

    private final String MY_ID;

    private final MongoCollection<Document> mongoCollection;

    private final ZkNodeWatcher zkNodeWatcher;

    @Autowired
    public MongoConsumer(MongoDatabase mongoDatabase, @Value("${my.id}") String my_id, @Value("${db.mongo.queue}") String db_mongo_coll, ZkNodeWatcher zkNodeWatcher) {
        this.mongoDatabase = mongoDatabase;
        MY_ID = my_id;
        this.zkNodeWatcher = zkNodeWatcher;
        this.mongoCollection = mongoDatabase.getCollection(db_mongo_coll);
    }

    public void subscribe(String collection) throws InterruptedException {
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
        observable.buffer(10).subscribe(
                this::processObservedBuffered
        );
        ;
//        observable.forEach(t -> {
//            processObserved(t);
//            LOG.trace(t.getFullDocument().toJson());
//        });
    }

    private void processObservedBuffered(List<ChangeStreamDocument<Document>> l) {
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        List<String> mongoIds = l.stream().map(t -> "ObjectId('" + t.getFullDocument().getObjectId("_id").toString() + "')").collect(Collectors.toList());
        UpdateResult results = Observable.fromPublisher(mongoCollection.updateMany(Document.parse("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}"), Document.parse("{$set : { status: 'processing'}}"))).blockingFirst();
        LOG.info("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}");
        LOG.info(results.toString());
    }

    private void processObserved(ChangeStreamDocument<Document> t) {
        Document updateStatus = new Document("status", "processing");
        long cnt = Observable.fromPublisher(mongoCollection.updateOne(Document.parse(t.getFullDocument().toJson()),
                Document.parse("{$set : { status: 'processing'}}"))).blockingFirst().getModifiedCount();
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        LOG.info("Node {} processed {}", processNodePath, t.getFullDocument().get("assigned"));
    }

}
