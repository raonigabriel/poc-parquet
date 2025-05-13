package com.github.raonigabriel.poc_parquet.service;

import java.io.FileWriter;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Profile("!test")
@RequiredArgsConstructor
public class CommandExecutor implements CommandLineRunner {
    
	private static final String TMP_PARQUET_FILE = "/tmp/movies.parquet";

	private final MovieService service;

	@Override
	public void run(String... args) throws Exception {
		// Setup and cleanup. We will end up with a database with 48 records and a bucket with a CSV file (50 records)
		service.ensureCleanBucket(MovieService.MOVIES_BUCKET);
		service.ensureCleanBucket(MovieService.WAREHOUSE_BUCKET);
		service.uploadMoviesCsv();
		service.ensureDefaultDatabase();

		// READ from PG, save to parquet
		var movies = service.readMoviesFromDatabase();
		var count = service.writeMoviesToParquet(TMP_PARQUET_FILE, movies);
		log.info("Exported {} movies from PG to Parquet file", count);
		
		// READ from remote CSV, save to parquet
		final var csvReader = service.downloadMoviesCsv();
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

		// READ from PG, save to Iceberg
		movies = service.readMoviesFromDatabase();
		count = service.writeMoviesToIceberg(movies);
		log.info("Exported {} movies from PG to Iceberg table", count);

		// READ from Iceberg, save to PG
		movies = service.readMoviesFromIceberg();
		for (var movie : movies) {
			movie.setId(null);
		}
		service.cleanDatabase();
		count = service.writeMoviesToDatabase(movies);
		log.info("Exported {} movies from Iceberg file to PG", count);
		
	}

}
