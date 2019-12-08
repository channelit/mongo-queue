package biz.cits.idepotent;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.connection.ClusterType;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.reactivex.Observable;
import org.bson.BsonDocument;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoReactiveRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


@SpringBootApplication(exclude = {MongoReactiveAutoConfiguration.class, MongoReactiveRepositoriesAutoConfiguration.class, MongoAutoConfiguration.class, MongoRepositoriesAutoConfiguration.class, MongoDataAutoConfiguration.class})
@EnableAspectJAutoProxy
public class App {

    @Value("${db.mongo.host}")
    private String DB_MONGO_HOST;

    @Value("${db.mongo.port}")
    private Integer DB_MONGO_PORT;

    @Value("${db.mongo.name}")
    private String DB_MONGO_NAME;

    @Value("${db.mongo.user}")
    private String DB_MONGO_USER;

    @Value("${db.mongo.pswd}")
    private String DB_MONGO_PSWD;

    @Value("${db.mongo.conn.max}")
    private Integer DB_MONGO_CONN_MAX;

    @Value("${db.mongo.conn.min}")
    private Integer DB_MONGO_CONN_MIN;

    @Value("${zk.znode.folder}")
    private String ZK_ZNODE_FOLDER;

    @Value("${zk.connect.url}")
    private String ZK_CONNECT_URL;

    @Value("${db.mongo.queue}")
    private String queue_db;

    @Bean
    public MongoClient mongoClient() {
//        MongoCredential mongoCredential = MongoCredential.createCredential(DB_MONGO_USER, "admin", DB_MONGO_PSWD.toCharArray());

        List<ServerAddress> hosts = Arrays.asList(new ServerAddress(DB_MONGO_HOST, 27017));
        MongoClient mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(hosts).requiredReplicaSetName("fifo").requiredClusterType(ClusterType.REPLICA_SET)
                        )
                        .applyToConnectionPoolSettings(block -> block
                                .maxSize(DB_MONGO_CONN_MAX)
                                .minSize(DB_MONGO_CONN_MIN))
//                        .credential(mongoCredential)
                        .build());

        return mongoClient;
    }

    @Bean
    public MongoDatabase mongoDatabase(@Qualifier("mongoClient") MongoClient mongoClient) {
        MongoDatabase db = mongoClient.getDatabase(DB_MONGO_NAME);
        Observable<Success> observable = Observable.fromPublisher(db.createCollection(queue_db));
        observable.blockingFirst();
        Observable<String> ttlIndex = Observable.fromPublisher(db.getCollection(queue_db).createIndex(Indexes.ascending("createdAt"), new IndexOptions().expireAfter(1L, TimeUnit.MINUTES)));
        ttlIndex.blockingFirst();
        return mongoClient.getDatabase(DB_MONGO_NAME);
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(App.class, args);
    }


    public class CommandLineRunnerBean implements CommandLineRunner {
        @Override
        public void run(String... args) {
        }
    }

    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsOfApp() {
        return registry -> registry.config().commonTags("app.name", "app-processor");
    }

    @Bean
    TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }
}
