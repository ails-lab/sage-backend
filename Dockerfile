FROM openjdk:8-slim

RUN mkdir /app
COPY target/gs-rest-service*.jar /app/gs-rest-service.jar

CMD java -jar /app/gs-rest-service.jar
