package biz.cits.idepotent;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.selector.ServerSelector;
import io.reactivex.Observable;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoReactiveRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SpringBootApplication(exclude = {MongoReactiveAutoConfiguration.class, MongoReactiveRepositoriesAutoConfiguration.class, MongoAutoConfiguration.class, MongoRepositoriesAutoConfiguration.class, MongoDataAutoConfiguration.class})
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

    @Value("${zk.znode.folder}")
    private String ZK_ZNODE_FOLDER;

    @Value("${zk.connect.url}")
    private String ZK_CONNECT_URL;

    @Bean
    public MongoClient mongoClient() {
//        MongoCredential mongoCredential = MongoCredential.createCredential(DB_MONGO_USER, "admin", DB_MONGO_PSWD.toCharArray());

        List<ServerAddress> hosts = Arrays.asList(new ServerAddress(DB_MONGO_HOST, 27019));
        MongoClient mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(hosts).requiredReplicaSetName("fifo").requiredClusterType(ClusterType.REPLICA_SET)
                        )
//                        .credential(mongoCredential)
                        .build());
        Observable<ClientSession> observable = Observable.fromPublisher(mongoClient.startSession());
        observable.blockingFirst();
        return mongoClient;
    }

    @Bean
    public MongoDatabase mongoDatabase(@Qualifier("mongoClient") MongoClient mongoClient) {
        return mongoClient.getDatabase(DB_MONGO_NAME);
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(App.class, args);
    }
}
