package biz.cits.idepotent.queue.consumer;

import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.observers.DisposableObserver;
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


    public void processDocuments() {
        Document d = null;
        FindPublisher<Document> publisher = mongoCollection.find(eq("status", "new"));
        Observable<Document> observable = Observable.fromPublisher(publisher);

        Observer<Document> observer = observable.subscribeWith(new Observer<Document>() {
            @Override
            public void onSubscribe(Disposable disposable) {

            }

            @Override
            public void onNext(Document document) {
                System.out.println(document.toJson());
                mongoDatabase.getCollection("processed").insertOne(document);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                System.out.println("Completed");
            }
        });

    }
}
