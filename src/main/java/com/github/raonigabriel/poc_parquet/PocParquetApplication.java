package com.github.raonigabriel.poc_parquet;

import java.io.FileWriter;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.github.raonigabriel.poc_parquet.service.MovieService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class PocParquetApplication implements CommandLineRunner {

	private static final String TMP_PARQUET_FILE = "/tmp/movies.parquet";

	private final MovieService service;

	public static void main(String[] args) {
		SpringApplication.run(PocParquetApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// Setup and cleanup. We will end up with a database with 48 records and a bucket with a CSV file (50 records)
		service.ensureCleanBucket();
		service.uploadComedyMoviesCsv();
		service.ensureDefaultDatabase();

		// READ from PG, save to parquet
		var movies = service.readMoviesFromDatabase();
		var count = service.writeMoviesToParquet(TMP_PARQUET_FILE, movies);
		log.info("Exported {} movies from PG to Parquet file", count);
		
		// READ from remote CSV, save to parquet
		final var csvReader = service.downloadComedyMoviesCsv();
		movies = service.readMoviesFromCsv(csvReader);
		count = service.writeMoviesToParquet(TMP_PARQUET_FILE, movies);
		log.info("Exported {} movies from CSV to Parquet file", count);

		// READ from parquet, save to PG
		movies = service.readMoviesFromParquet(TMP_PARQUET_FILE);
		count = service.writeMoviesToDatabase(movies);
		log.info("Exported {} movies from Parquet file to PG", count);

		// READ from PG, save to local CSV
		movies = service.readMoviesFromDatabase();
		final var csvWriter = new FileWriter("/tmp/movies.csv");
		count = service.writeMoviesToCsv(csvWriter, movies);
		log.info("Exported {} movies from PG to CSV file", count);
	}

}
