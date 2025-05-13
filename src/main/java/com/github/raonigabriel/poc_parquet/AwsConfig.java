package com.github.raonigabriel.poc_parquet;

import java.net.URI;
import java.net.URISyntaxException;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Slf4j
@Configuration
@Profile("!test")
public class AwsConfig {

	@Value("${aws.s3.endpoint}")
    String s3Endpoint;

	@Bean
	@ConditionalOnMissingBean
	S3Client s3Client() {
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
				.endpointOverride(new URI(s3Endpoint)) 
				.region(Region.US_EAST_1)
				.forcePathStyle(true)
			    .httpClientBuilder(ApacheHttpClient.builder())
			    .build();
		} catch (URISyntaxException ex) {
			throw new BeanInitializationException("Failed to create S3 client", ex);
		}
	}
}
