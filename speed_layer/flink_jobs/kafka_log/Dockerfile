FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests package

FROM flink:1.20.2-java17
COPY --from=build /workspace/target/socialrec-flink-kafka-log.jar /opt/flink/usrlib/socialrec-flink-kafka-log.jar
