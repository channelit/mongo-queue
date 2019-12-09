package biz.cits.idepotent.queue.worker;

import biz.cits.idepotent.queue.consumer.MongoSubscriber;
import biz.cits.idepotent.queue.message.BaseProcessor;
import biz.cits.idepotent.queue.zk.ZkNodeWatcher;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.micrometer.core.annotation.Timed;
import io.reactivex.Observable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MasterProcessor implements BaseProcessor<ChangeStreamDocument<Document>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSubscriber.class);
    private final MongoCollection<Document> mongoCollection;
    private final ZkNodeWatcher zkNodeWatcher;

    @Autowired
    public MasterProcessor(MongoDatabase mongoDatabase, @Value("${db.mongo.queue}") String db_mongo_coll, ZkNodeWatcher zkNodeWatcher) {
        this.zkNodeWatcher = zkNodeWatcher;
        this.mongoCollection = mongoDatabase.getCollection(db_mongo_coll);
    }

    @Override
    @Timed(value = "processor")
    public void processObservedBuffered(List<ChangeStreamDocument<Document>> l) {
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        List<String> mongoIds = l.stream().map(t -> "ObjectId('" + t.getDocumentKey().getObjectId("_id").getValue().toString() + "')").collect(Collectors.toList());
        UpdateResult results = Observable.fromPublisher(mongoCollection.updateMany(Document.parse("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}"), Document.parse("{$set : { status: 'processing'}}"))).blockingFirst();
        LOG.debug("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        results = Observable.fromPublisher(mongoCollection.updateMany(Document.parse("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}"), Document.parse("{$set : { status: 'processed'}}"))).blockingFirst();
        LOG.info("Node {} processed {}", processNodePath, results.getModifiedCount());
    }

    @Override
    public void processObserved(ChangeStreamDocument<Document> t) {
        Observable.fromPublisher(mongoCollection.updateOne(Document.parse(t.getFullDocument().toJson()),
                Document.parse("{$set : { status: 'processing'}}"))).blockingFirst().getModifiedCount();
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        long cnt = Observable.fromPublisher(mongoCollection.updateOne(Document.parse(t.getFullDocument().toJson()),
                Document.parse("{$set : { status: 'processed'}}"))).blockingFirst().getModifiedCount();
        LOG.info("Node {} processed {}", processNodePath, cnt);
    }
}
