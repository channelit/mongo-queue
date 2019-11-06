### Queue example for distributed application using Mongo Reactive Stream
#### Uses ZooKeeper for leader application (optional)
#### Dependencies used in this example
- ## Spring Boot
- ## RxJava 2
- ## MongoDB Reactive API

```shell script
docker cp replica.js mongo1:/.
docker exec mongo1 bash -c 'mongo < /replica.js'
```