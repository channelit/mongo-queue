### Queue example for distributed application using Mongo Reactive Stream
#### Uses ZooKeeper for leader application (optional)
#### Dependencies used in this example
- ## Spring Boot
- ## RxJava 2
- ## MongoDB Reactive API
##### Build the app:

```shell script
mvn clean install
docker build . -t mongo-queue
docker tag mongo-queue mongo-queue:v1
```

```shell script
docker cp docker/config/mongo/replica.js mongo1:/.
docker exec mongo1 bash -c 'mongo < /replica.js'
docker cp docker/config/mongo/replica.js mongo2:/.
docker exec mongo2 bash -c 'mongo --host localhost:27019 < /replica.js'
```