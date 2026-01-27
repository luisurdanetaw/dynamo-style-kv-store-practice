# Build stage
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

# Runtime stage
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app/target/kv-node-0.1.0.jar /app/kv-node.jar
COPY --from=build /app/target/deps /app/deps
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["java","-cp","/app/kv-node.jar:/app/deps/*","com.luisurdaneta.kv.Main"]