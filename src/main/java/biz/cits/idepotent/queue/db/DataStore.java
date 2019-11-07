package biz.cits.idepotent.queue.db;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import io.reactivex.Observable;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component
public class DataStore {

    @Value("${my.id}")
    private String MY_ID;

    private final MongoDatabase mongoDatabase;


    private final String QUEUE_DB = "queue";

    @Autowired
    public DataStore(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public void queueData(String key, HashMap<String, String> records) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(QUEUE_DB);
        Document doc = new Document("key", key);
        records.forEach((k, v) -> {
            doc.append("data", v);
        });
        doc.append("source", MY_ID);
        doc.append("status", "new");
        Observable<Success> observable = Observable.fromPublisher(collection.insertOne(doc));
        Success result = observable.blockingFirst();
        System.out.println(result.toString());
    }

    public void storeData(String collectionName, HashMap<String, String> records) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document doc = new Document("client", collectionName);
        records.forEach(doc::append);
        doc.append("source", MY_ID);
        Observable<Success> observable = Observable.fromPublisher(collection.insertOne(doc));
        Success result = observable.blockingFirst();
        System.out.println(result.toString());
    }
}
