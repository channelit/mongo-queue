package biz.cits.idepotent.queue.consumer;

import biz.cits.mongo.SubscriberHelpers;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static com.mongodb.client.model.Filters.eq;

@Component
public class MongoConsumer {

    private final MongoDatabase mongoDatabase;

    @Value("${db.mongo.coll}")
    String DB_MONGO_COLL;

    private final MongoCollection<Document> mongoCollection;

    @Autowired
    public MongoConsumer(MongoDatabase mongoDatabase, @Value("${db.mongo.coll}") String DB_MONGO_COLL) {
        this.mongoDatabase = mongoDatabase;
        this.mongoCollection = mongoDatabase.getCollection(DB_MONGO_COLL);

    }


    public void processDocuments() throws Throwable {
        mongoDatabase.getCollection(DB_MONGO_COLL);
        SubscriberHelpers.ObservableSubscriber subscriber = new SubscriberHelpers.ObservableSubscriber<Success>();
        mongoCollection.find(eq("status", "new")).subscribe(subscriber);
        subscriber.await();

    }
}
