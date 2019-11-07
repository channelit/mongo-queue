package biz.cits.idepotent.queue.consumer;

import com.mongodb.MongoTimeoutException;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import io.reactivex.Observable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;

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
        observable.forEach(t->{
            processObserved(t);
            System.out.println(t.getFullDocument().toJson());
        });
//        ObservableSubscriber<ChangeStreamDocument<Document>> subscriber = new ObservableSubscriber<>(true);
//        publisher.subscribe(subscriber);
//
//        subscriber.waitForThenCancel(10);

    }

    private void processObserved(ChangeStreamDocument<Document> t) {
        Document updateStatus = new Document("status", "processing");
        long cnt = Observable.fromPublisher(mongoCollection.updateOne(Document.parse(t.getFullDocument().toJson()),
                Document.parse("{$set : { status: 'processing'}}"))).blockingFirst().getModifiedCount();
        System.out.println("updated " + cnt);

    }

    private static class ObservableSubscriber<T> implements Subscriber<T> {
        private final CountDownLatch latch;
        private final List<T> results = new ArrayList<T>();
        private final boolean printResults;

        private volatile int minimumNumberOfResults;
        private volatile int counter;
        private volatile Subscription subscription;
        private volatile Throwable error;

        public ObservableSubscriber(final boolean printResults) {
            this.printResults = printResults;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
            subscription.request(Integer.MAX_VALUE);
        }

        @Override
        public void onNext(final T t) {
            results.add(t);
            if (printResults) {
                System.out.println(">>>>>>>>> t >>>>>>>" + t);
            }
            counter++;
            if (counter >= minimumNumberOfResults) {
                latch.countDown();
            }
        }

        @Override
        public void onError(final Throwable t) {
            error = t;
            System.out.println(t.getMessage());
            onComplete();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }

        public List<T> getResults() {
            return results;
        }

        public void await() throws Throwable {
            if (!latch.await(10, SECONDS)) {
                throw new MongoTimeoutException("Publisher timed out");
            }
            if (error != null) {
                throw error;
            }
        }

        public void waitForThenCancel(final int minimumNumberOfResults) throws Throwable {
            this.minimumNumberOfResults = minimumNumberOfResults;
            if (minimumNumberOfResults > counter) {
                await();
            }
            subscription.request(10);
        }
    }

    private static <T> void subscribeAndAwait(final Publisher<T> publisher) throws Throwable {
        ObservableSubscriber<T> subscriber = new ObservableSubscriber<T>(false);
        publisher.subscribe(subscriber);
        subscriber.await();
    }


}
