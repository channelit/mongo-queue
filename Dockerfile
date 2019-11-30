FROM openjdk:11-jre-slim
COPY ./target/mongo-queue-1.0-SNAPSHOT.jar /app/
WORKDIR /app
RUN sh -c 'touch mongo-queue-1.0-SNAPSHOT.jar'
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "mongo-queue-1.0-SNAPSHOT.jar"]