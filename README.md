# poc-parquet

Demo project for testing Parquet, S3, and Postgres using Java.

This project is a POC for a CLI app (using Java 21, Maven, and SpringBoot). The main purpose is to ensure we can build (and package) an arm64 version of it.

## Rationale
We already have very good support for JDK on arm64 (on Docker, either Alpine or Debian-based images have JDKs). The incompatibility issues usually arise from the use of external dependencies (libraries) that do not work very well with the arm64 architecture. This is particularly important for libraries that rely on native code and wrappers like JNI/JNA. Another issue arises from native libraries being linked to different native "libc" implementations (muslc on Alpine, glibc on Debian). Finally, some issues arise from incompatibilities between logging libraries like log4j, jul, common-logging, reload4j.

Therefore, this POC is a CLI app that should:
- Read/Write from S3
- Read/Write CSV files
- Read/Write Parquet files
- Read/Write from Postgres
- Load and use all required libraries successfully
- Package the app as an "uber-jar"
- Multi-platform builds using Docker "buildx" + QEMU both locally and with GitHub Actions


## Secondary Objectives (desired, but not required)
- GraalVM native-compile 
- Build and use native "libhadoop.so"
- Use JLink to create a custom-tailored JRE
- Use Picocli to parse commad line args
- Reduce Hadoop bloat: identify useless dependencies and add them to exclusion list


## Prerequisites
- JDK 21 installed
- Postgres and localstack:
   ```bash
   docker compose up
   ```


## For local multi-arch builds, we need:
  -  QEMU installed 
   ```bash
   sudo apt install qemu-system
   ```
  - Docker installed with support for "buildx". Follow steps [here](https://docs.docker.com/build/building/multi-platform/#qemu) and also [here](https://cloudolife.com/2022/03/05/Infrastructure-as-Code-IaC/Container/Docker/Docker-buildx-support-multiple-architectures-images)


## Build Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/raonigabriel/poc-parquet.git
   cd poc-parquet
   ```
2. Build the project:
   ```bash
   ./mvnw clean test package
   ```
3. Run the application:
   ```bash
   java -jar target/poc-parquet-0.1.0.jar
   ```

## Local build (to your native arch)
   ```bash
   docker build -t poc-parquet
   ```

## Build multi-platform images
   ```bash
   docker buildx build --platform linux/amd64,linux/arm64 .
   ```


## License

Code is released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html)
