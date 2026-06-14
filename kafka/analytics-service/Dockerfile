# Build context is the repository root (see docker-compose.yml).
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

COPY kafka/analytics-service/pom.xml ./pom.xml
RUN mvn -q -B dependency:go-offline

COPY kafka/analytics-service/src ./src
RUN mvn -q -B clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
