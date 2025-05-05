# This is a hack to setup alternate architecture names, using a single Dockerfile
# For this to work, it needs to be built using docker 'buildx'
FROM azul/zulu-openjdk-debian:21-latest AS linux-amd64-builder
ARG ALT_ARCH=x64

FROM azul/zulu-openjdk-debian:21-latest AS linux-arm64-builder
ARG ALT_ARCH=arm64

FROM azul/zulu-openjdk-debian:21-jre-headless-latest AS linux-amd64-runtime
ARG ALT_ARCH=x64

FROM azul/zulu-openjdk-debian:21-jre-headless-latest AS linux-arm64-runtime
ARG ALT_ARCH=arm64

# This inherits from the hack above
FROM ${TARGETOS}-${TARGETARCH}-builder AS builder
ARG TARGETARCH

# Warmup the maven cache
COPY --chown=1000:1000 ./pom.xml /app/pom.xml
COPY --chown=1000:1000 ./mvnw /app/mvnw
COPY --chown=1000:1000 ./.mvn /app/.mvn
WORKDIR /app
RUN ./mvnw dependency:go-offline

# Build the application
COPY --chown=1000:1000 ./src /app/src
RUN ./mvnw clean package

FROM ${TARGETOS}-${TARGETARCH}-runtime
WORKDIR /app

# Install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends bzip2 ca-certificates curl libsnappy1v5 zlib1g zstd && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/poc-parquet-*.jar /app/poc-parquet.jar