package biz.cits.idepotent.queue.consumer;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.Observable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

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

    public void subscribe(String collection) throws Throwable {
        List<Bson> updatePipeline = Collections.singletonList(
                Aggregates.match(
                        Filters.and(
                                Document.parse("{'fullDocument.status':'new'}")
                        )
                )
        );
        ChangeStreamPublisher<Document> publisher = mongoDatabase.getCollection(collection).watch(updatePipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
        Observable<ChangeStreamDocument<Document>> observable = Observable.fromPublisher(publisher);
        observable.buffer(10);
        observable.forEach(t -> {
            processObserved(t);
            System.out.println(t.getFullDocument().toJson());
        });
    }

    private void processObserved(ChangeStreamDocument<Document> t) {
        Document updateStatus = new Document("status", "processing");
        long cnt = Observable.fromPublisher(mongoCollection.updateOne(Document.parse(t.getFullDocument().toJson()),
                Document.parse("{$set : { status: 'processing'}}"))).blockingFirst().getModifiedCount();
        System.out.println("updated " + cnt);
    }

}
