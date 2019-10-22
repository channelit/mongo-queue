package biz.cits.idepotent.queue.db;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component
public class DataStore {

    @Value("${my.id}")
    private String MY_ID;

    private final MongoDatabase mongoDatabase;

    @Autowired
    public DataStore(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    public void queueData(String key, HashMap<String, String> records) {
        MongoCollection collection = mongoDatabase.getCollection("queue");
        Document doc = new Document("key", key);
        records.forEach((k, v) -> {
            doc.append("data", v);
        });
        doc.append("source", MY_ID);
        doc.append("status", "new");
        collection.insertOne(doc).subscribe(new Subscriber<Success>() {
            @Override
            public void onSubscribe(final Subscription s) {
                s.request(1);  // <--- Data requested and the insertion will now occur
            }

            @Override
            public void onNext(final Success success) {
                System.out.println("Inserted");
            }

            @Override
            public void onError(final Throwable t) {
                System.out.println("Failed");
            }

            @Override
            public void onComplete() {
                System.out.println("Completed");
            }
        });

    }

    public void storeData(String collectionName, HashMap<String, String> records) {
        MongoCollection collection = mongoDatabase.getCollection(collectionName);
        Document doc = new Document("client", collectionName);
        records.forEach(doc::append);
        doc.append("source", MY_ID);
        collection.insertOne(doc).subscribe(new Subscriber<Success>() {
            @Override
            public void onSubscribe(final Subscription s) {
                s.request(1);  // <--- Data requested and the insertion will now occur
            }

            @Override
            public void onNext(final Success success) {
                System.out.println("Inserted");
            }

            @Override
            public void onError(final Throwable t) {
                System.out.println("Failed");
            }

            @Override
            public void onComplete() {
                System.out.println("Completed");
            }
        });

    }
}
