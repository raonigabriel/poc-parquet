package com.github.raonigabriel.poc_parquet.service;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.core.io.Resource;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.github.raonigabriel.poc_parquet.model.MovieEntity;
import com.github.raonigabriel.poc_parquet.repository.MovieRepository;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriterBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.EncryptionTypeMismatchException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.InvalidRequestException;
import software.amazon.awssdk.services.s3.model.InvalidWriteOffsetException;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.TooManyPartsException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.avro.AvroParquetReader;

@Slf4j
@Service
@RequiredArgsConstructor
public class MovieService {

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String RATING_FIELD = "rating";
    private static final String RELEASE_DATE_FIELD = "releaseDate";

    private static final String BUCKET_NAME = "movies-bucket";

    private final MovieRepository movieRepository;

    private static final String EXTRA_MOVIES_CSV = "extra_movies.csv";

    private final S3Client s3Client;

    public int writeMoviesToParquet(String fileName, List<MovieEntity> movies) {

        final var schema = SchemaBuilder.record("Movie")
            .fields()
            .optionalLong(ID_FIELD)
            .requiredString(NAME_FIELD)
            .requiredFloat(RATING_FIELD)
            .requiredString(RELEASE_DATE_FIELD)
            .endRecord();

        final var path = new Path(fileName);
        
        OutputFile outputFile;
        try {
            outputFile = HadoopOutputFile.fromPath(path, new Configuration());
        } catch (IOException ex) {
            throw new RuntimeException("Error creating Parquet file", ex);
        }

        int count = 0;
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withWriteMode(Mode.OVERWRITE)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                .build()) {

            for (final var movie : movies) {
                final GenericRecord movieRecord = new GenericData.Record(schema);
                movieRecord.put(ID_FIELD, movie.getId());
                movieRecord.put(NAME_FIELD, movie.getName());
                movieRecord.put(RATING_FIELD, movie.getRating());
                movieRecord.put(RELEASE_DATE_FIELD, movie.getReleaseDate().toString());
                writer.write(movieRecord);
                count++;
            }
            return count;
        } catch (Exception ex) {
            throw new RuntimeException("Error writing Parquet file", ex);
        }
    }

    public List<MovieEntity> readMoviesFromParquet(String fileName) {
        final var path = new Path(fileName);
        InputFile inputFile;
        
        try {
            inputFile = HadoopInputFile.fromPath(path, new Configuration());
        } catch (IOException ex) {
            throw new RuntimeException("Error opening Parquet file", ex);
        }
        final var movies = new ArrayList<MovieEntity>();
        try (var reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord movieRecord;
            while ((movieRecord = reader.read()) != null) {
                final MovieEntity movie = new MovieEntity();
                movie.setId((Long) movieRecord.get(ID_FIELD));
                movie.setName(movieRecord.get(NAME_FIELD).toString());
                movie.setRating((Float) movieRecord.get(RATING_FIELD));
                movie.setReleaseDate(LocalDate.parse(movieRecord.get(RELEASE_DATE_FIELD).toString()));
                movies.add(movie);
            }
            log.info("Successfully read {} movies from Parquet file", movies.size());
            return movies;
        } catch (IOException ex) {
            throw new RuntimeException("Error reading Parquet file", ex);
        }
    }

    public List<MovieEntity> readMoviesFromCsv(Reader reader) {
        try (final var csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()) {
                return csvReader.readAll().stream().map(this::mapToMovieEntity).toList();
        } catch (Exception ex) {
            throw new RuntimeException("Error reading CSV", ex);
        }
    }

    public int writeMoviesToCsv(Writer writer, List<MovieEntity> movies) {
        log.info("Writing {} movies to CSV", movies.size());
        int count = 0;
        try (final var csvWriter = new CSVWriterBuilder(writer).build()) {
            csvWriter.writeNext(new String[] { ID_FIELD, NAME_FIELD, RATING_FIELD, RELEASE_DATE_FIELD });
            for (final var movie : movies) {
                final var data = new String[] {
                    movie.getId() == null ? "" : String.valueOf(movie.getId()),
                    movie.getName(),
                    String.valueOf(movie.getRating()),
                    movie.getReleaseDate().toString()
                };
                csvWriter.writeNext(data);
                count++;
            }
            csvWriter.flush();
            return count;
        } catch (Exception ex) {
            throw new RuntimeException("Error reading CSV", ex);
        }
    }

    @Transactional(readOnly = true)
    public List<MovieEntity> readMoviesFromDatabase() {
        final var movies = movieRepository.findAll();
        log.info("Successfully read {} movies from PG", movies.size());
        return movies;
    }

    @Transactional(readOnly = false)
    public void ensureDefaultDatabase() {
        final long defaultCount = 48L;
        if (movieRepository.count() > defaultCount) {
            log.info("Database has extra data, keeping only default data");
            movieRepository.deleteAllByIdGreaterThan(defaultCount);
        }
    }

    public void ensureCleanBucket() {
        final boolean bucketExists = s3Client.listBuckets()
            .buckets()
            .stream()
            .anyMatch(b -> b.name().equals(BUCKET_NAME));

        if (bucketExists) {
            log.info("Bucket {} already exists, removing it", BUCKET_NAME);
            final var listFiles = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build();
            log.info("Removing all files (if any) from bucket {}", BUCKET_NAME);
            s3Client.listObjectsV2Paginator(listFiles).contents()
                .forEach(obj -> s3Client.deleteObject(b -> b.bucket(BUCKET_NAME).key(obj.key())));
            s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET_NAME).build());
        } else {
            log.info("Bucket {} does not exists", BUCKET_NAME);
        }
        s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
        log.info("Created a new bucket {}", BUCKET_NAME);
    }

    public void uploadMoviesCsv() {
        final var classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(EXTRA_MOVIES_CSV)) {
            if (inputStream == null) {
                throw new BeanInitializationException(BUCKET_NAME);
            }
            uploadFile(EXTRA_MOVIES_CSV, inputStream, "text/csv");
        } catch (IOException ex) {
            throw new RuntimeException("Error uploading comedy movies CSV file", ex);
        }
    }

    public Reader downloadMoviesCsv() {
        try {
        final var getRequest = GetObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(EXTRA_MOVIES_CSV)
            .build();
            return new InputStreamReader(s3Client.getObject(getRequest));
        } catch (Exception ex) {
            throw new RuntimeException("Error downloading comedy movies CSV file", ex);
        }
    }

    private void uploadFile(String fileName, InputStream srcData, String contentType) throws IOException {
        final var putRequest = PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(fileName)
            .contentType(contentType)
            .build();
        s3Client.putObject(putRequest, RequestBody.fromBytes(srcData.readAllBytes()));
        log.info("Uploaded file {} to bucket {}", fileName, BUCKET_NAME);
    }

    private MovieEntity mapToMovieEntity(String[] data) {
        final var movie = new MovieEntity();
        if (StringUtils.hasText(data[0])) {
            movie.setId(Long.parseLong(data[0]));
        }
        movie.setName(data[1]);
        movie.setRating(Float.parseFloat(data[2]));
        movie.setReleaseDate(LocalDate.parse(data[3]));
        return movie;
    }

    @Modifying
    @Transactional(readOnly = false)
    public int writeMoviesToDatabase(List<MovieEntity> movies) {
        if (movies == null || movies.isEmpty()) {
            return 0;
        } else {
            return movieRepository.saveAll(movies).size();
        }
    }

}
