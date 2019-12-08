package biz.cits.idepotent.queue.db;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import io.reactivex.Observable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;

@Component
public class DataStore {

    private final String MY_ID;
    private final MongoDatabase mongoDatabase;
    private final String QUEUE_DB;

    private static final Logger LOG = LoggerFactory.getLogger(DataStore.class);

    @Autowired
    public DataStore(@Value("${my.id}") String my_id, MongoDatabase mongoDatabase, @Value("${db.mongo.queue}") String queue_db) {
        MY_ID = my_id;
        this.mongoDatabase = mongoDatabase;
        QUEUE_DB = queue_db;
    }

    public void queueData(String key, HashMap<String, String> records) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(QUEUE_DB);
        Document doc = new Document("key", key);
        records.forEach(doc::append);
        doc.append("source", MY_ID);
        doc.append("status", "new");
        doc.append("createdAt", new Date());
        Observable<Success> observable = Observable.fromPublisher(collection.insertOne(doc));
        Success result = observable.blockingFirst();
        LOG.trace("Message sent {} {}", result.toString(), doc.toJson());
    }
}
