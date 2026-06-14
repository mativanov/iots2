# Build context is the repository root (see docker-compose.yml) so we can copy
# the shared CSV dataset into the image, mirroring the MQTT ingestion service.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Cache dependencies first for faster rebuilds.
COPY kafka/ingestion-service/pom.xml ./pom.xml
RUN mvn -q -B dependency:go-offline

COPY kafka/ingestion-service/src ./src
RUN mvn -q -B clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
COPY shared/sample-data /app/sample-data
ENTRYPOINT ["java", "-jar", "app.jar"]
