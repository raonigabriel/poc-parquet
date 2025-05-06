FROM ubuntu:24.10 AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends bzip2 ca-certificates curl \
    libsnappy1v5 openjdk-21-jre-headless zlib1g zstd && \
    rm -rf /var/lib/apt/lists/*

FROM runtime AS builder
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Warmup the maven cache
COPY --chown=1000:1000 ./pom.xml /app/pom.xml
COPY --chown=1000:1000 ./mvnw /app/mvnw
COPY --chown=1000:1000 ./.mvn /app/.mvn
WORKDIR /app
RUN ./mvnw dependency:go-offline

# Build the application
COPY --chown=1000:1000 ./src /app/src
RUN ./mvnw clean package -Dmaven.test.skip=true

FROM runtime
WORKDIR /app

COPY --from=builder /app/target/poc-parquet-*.jar /app/poc-parquet.jar

CMD ["java", "-jar", "/app/poc-parquet.jar"]