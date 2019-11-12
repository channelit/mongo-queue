package biz.cits.idepotent.queue.work;

import biz.cits.idepotent.queue.consumer.MongoConsumer;
import biz.cits.idepotent.queue.message.BaseProcessor;
import biz.cits.idepotent.queue.zk.ZkNodeWatcher;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
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

    private static final Logger LOG = LoggerFactory.getLogger(MongoConsumer.class);

    private final MongoDatabase mongoDatabase;

    private final String MY_ID;

    private final MongoCollection<Document> mongoCollection;

    private final ZkNodeWatcher zkNodeWatcher;

    @Autowired
    public MasterProcessor(MongoDatabase mongoDatabase, @Value("${my.id}") String my_id, @Value("${db.mongo.queue}") String db_mongo_coll, ZkNodeWatcher zkNodeWatcher) {
        this.mongoDatabase = mongoDatabase;
        MY_ID = my_id;
        this.zkNodeWatcher = zkNodeWatcher;
        this.mongoCollection = mongoDatabase.getCollection(db_mongo_coll);
    }

    @Override
    public void processObservedBuffered(List<ChangeStreamDocument<Document>> l) {
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        List<String> mongoIds = l.stream().map(t -> "ObjectId('" + t.getFullDocument().getObjectId("_id").toString() + "')").collect(Collectors.toList());
        UpdateResult results = Observable.fromPublisher(mongoCollection.updateMany(Document.parse("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}"), Document.parse("{$set : { status: 'processing'}}"))).blockingFirst();
        LOG.info("{_id: { $in:" + Arrays.toString(mongoIds.toArray()) + "}}");
        LOG.info(results.toString());
        LOG.info("Node {} processed {}", processNodePath, l.size());
    }

    @Override
    public void processObserved(ChangeStreamDocument<Document> t) {
        Document updateStatus = new Document("status", "processing");
        long cnt = Observable.fromPublisher(mongoCollection.updateOne(Document.parse(t.getFullDocument().toJson()),
                Document.parse("{$set : { status: 'processing'}}"))).blockingFirst().getModifiedCount();
        String processNodePath = zkNodeWatcher.getProcessNodePath();
        LOG.info("Node {} processed {}", processNodePath, t.getFullDocument().get("assigned"));
    }
}
