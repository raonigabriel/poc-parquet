package com.github.raonigabriel.poc_parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.github.raonigabriel.poc_parquet.service.MovieService;

import lombok.SneakyThrows;

@SpringBootTest
@Import(AwsTestConfig.class)
class MovieServiceTests {

	private static final String TMP_PARQUET_FILE = "/tmp/movies.parquet";

	@Autowired
	private MovieService service;

	@BeforeEach
	void setUp(){
		service.ensureCleanBucket();
		service.uploadComedyMoviesCsv();
		service.ensureDefaultDatabase();		
	}

	@Test
	void readPgWriteParquet() {
		final var movies = service.readMoviesFromDatabase();
		assertThat(movies).hasSize(48);
		final var exportedCount = service.writeMoviesToParquet(TMP_PARQUET_FILE, movies);
		assertThat(exportedCount).isEqualTo(48);
	}

	@Test
	void readCsvWriteParquet() {
		final var csvReader = service.downloadComedyMoviesCsv();
		final var movies = service.readMoviesFromCsv(csvReader);
		assertThat(movies).hasSize(50);
		final var exportedCount = service.writeMoviesToParquet(TMP_PARQUET_FILE, movies);
		assertThat(exportedCount).isEqualTo(50);
	}

	@Test
	void readParquetSavePg() {
		readCsvWriteParquet();
		final var movies =  service.readMoviesFromParquet(TMP_PARQUET_FILE);
		final var exportedCount = service.writeMoviesToDatabase(movies);
		assertThat(exportedCount).isEqualTo(50);
	}

	@Test
	@SneakyThrows
	void readPgSaveCsv(){
		final var movies = service.readMoviesFromDatabase();
		assertThat(movies).hasSize(48);
		final var csvWriter = new FileWriter("/tmp/movies.csv");
		final var exportedCount = service.writeMoviesToCsv(csvWriter, movies);
		assertThat(exportedCount).isEqualTo(48);
	}
}
