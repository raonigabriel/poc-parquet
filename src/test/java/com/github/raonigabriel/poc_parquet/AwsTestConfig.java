package com.github.raonigabriel.poc_parquet;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
@Configuration
public class AwsTestConfig {
    
    @Container
    @SuppressWarnings({"rawTypes", "resource"})
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:s3-latest")).withServices(S3);


	@Bean
    @Primary
	S3Client s3Client() {
        LOCALSTACK.start();
		log.info("Creating S3 client");

		final var credentials = new AwsCredentials() {
				@Override
				public String accessKeyId() {
					return "test";
				}
	
				@Override
				public String secretAccessKey() {
					return "test";
				}
			};

		try {
			return S3Client.builder()
				.credentialsProvider(StaticCredentialsProvider.create(credentials))
				.endpointOverride(new URI(LOCALSTACK.getEndpointOverride(S3).toString())) 
				.region(Region.US_EAST_1)
				.forcePathStyle(true)
			    .httpClientBuilder(ApacheHttpClient.builder())
			    .build();
		} catch (URISyntaxException ex) {
			throw new BeanInitializationException("Failed to create S3 client", ex);
		}
	}
}
