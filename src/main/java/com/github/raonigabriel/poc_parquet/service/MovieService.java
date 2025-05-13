package com.github.raonigabriel.poc_parquet.service;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import com.github.raonigabriel.poc_parquet.model.MovieEntity;
import com.github.raonigabriel.poc_parquet.repository.MovieRepository;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriterBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.avro.AvroParquetReader;

@Slf4j
@Service
@RequiredArgsConstructor
public class MovieService {

    public static final String ID_FIELD = "id";
    public static final String NAME_FIELD = "name";
    public static final String RATING_FIELD = "rating";
    public static final String RELEASE_DATE_FIELD = "releaseDate";

    public static final String MOVIES_BUCKET = "movies-bucket";
    public static final String WAREHOUSE_BUCKET = "warehouse";

    public static final String ICEBERG_TABLE = "movies";

    public static final String EXTRA_MOVIES_CSV = "extra_movies.csv";

    private final MovieRepository movieRepository;

    private final S3Client s3Client;

    private final DataSource dataSource;

    private final String s3EndpointOverride;

    public int writeMoviesToParquet(String fileName, List<MovieEntity> movies) {

        final var schema = SchemaBuilder.record("MovieEntity")
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

    @Transactional(readOnly = false)
    public void cleanDatabase() {
        movieRepository.deleteAllByIdGreaterThan(0L);
    }

    public void ensureCleanBucket(String bucketName) {
        final boolean bucketExists = s3Client.listBuckets()
            .buckets()
            .stream()
            .anyMatch(b -> b.name().equals(bucketName));

        if (bucketExists) {
            log.info("Bucket {} already exists, removing it", bucketName);
            final var listFiles = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
            log.info("Removing all files (if any) from bucket {}", bucketName);
            s3Client.listObjectsV2Paginator(listFiles).contents()
                .forEach(obj -> s3Client.deleteObject(b -> b.bucket(bucketName).key(obj.key())));
            s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
        } else {
            log.info("Bucket {} does not exists", bucketName);
        }
        s3Client.createBucket(b -> b.bucket(bucketName));
        log.info("Created a new bucket {}", bucketName);
    }

    public void uploadMoviesCsv() {
        final var classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(EXTRA_MOVIES_CSV)) {
            if (inputStream == null) {
                throw new BeanInitializationException(MOVIES_BUCKET);
            }
            uploadFile(EXTRA_MOVIES_CSV, inputStream, "text/csv");
        } catch (IOException ex) {
            throw new RuntimeException("Error uploading movies CSV file", ex);
        }
    }

    public Reader downloadMoviesCsv() {
        try {
        final var getRequest = GetObjectRequest.builder()
            .bucket(MOVIES_BUCKET)
            .key(EXTRA_MOVIES_CSV)
            .build();
            return new InputStreamReader(s3Client.getObject(getRequest));
        } catch (Exception ex) {
            throw new RuntimeException("Error downloading movies CSV file", ex);
        }
    }

    private void uploadFile(String fileName, InputStream srcData, String contentType) throws IOException {
        final var putRequest = PutObjectRequest.builder()
            .bucket(MOVIES_BUCKET)
            .key(fileName)
            .contentType(contentType)
            .build();
        s3Client.putObject(putRequest, RequestBody.fromBytes(srcData.readAllBytes()));
        log.info("Uploaded file {} to bucket {}", fileName, MOVIES_BUCKET);
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

    private String getJdbcCatalogUrl() {
        try (final var conn = dataSource.getConnection()) {
            return conn.getMetaData().getURL();
        } catch (SQLException ex) {
            throw new RuntimeException("Error getting catalog URL", ex);
        }
    }

    private String getJdbcCatalogUsername() {
        try (final var conn = dataSource.getConnection()) {
            return conn.getMetaData().getUserName();
        } catch (SQLException ex) {
            throw new RuntimeException("Error getting catalog username", ex);
        }
    }

    private String getJdbcCatalogPassword() {
        return "postgres";
    }

    public int writeMoviesToIceberg(List<MovieEntity> movies) {

        if (movies == null || movies.isEmpty()) {
            return 0;
        } else {
            try (JdbcCatalog catalog = new JdbcCatalog()) {
                final var props = new HashMap<String, String>();
    
                props.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
                props.put(CatalogProperties.URI, getJdbcCatalogUrl());
                props.put(JdbcCatalog.PROPERTY_PREFIX + "user", getJdbcCatalogUsername());
                props.put(JdbcCatalog.PROPERTY_PREFIX + "password", getJdbcCatalogPassword());
        
                props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + WAREHOUSE_BUCKET);
                props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
                props.put(S3FileIOProperties.ENDPOINT, s3EndpointOverride);
                props.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
                props.put(S3FileIOProperties.ACCESS_KEY_ID, "test");
                props.put(S3FileIOProperties.SECRET_ACCESS_KEY, "test");
                System.setProperty("aws.region", "us-east-1");
    
                catalog.setConf(new Configuration());
                catalog.initialize("MoviesCatalog", props);
        
                final var namespace = Namespace.of("public");
                final var schema = new Schema(
                    Types.NestedField.optional(1, MovieService.ID_FIELD, Types.LongType.get()),
                    Types.NestedField.required(2, MovieService.NAME_FIELD, Types.StringType.get()),
                    Types.NestedField.required(3, MovieService.RATING_FIELD, Types.FloatType.get()),
                    Types.NestedField.required(4, MovieService.RELEASE_DATE_FIELD, Types.DateType.get())
                );
        
                final var partitionConfig = PartitionSpec.unpartitioned();
                final var tableIdentifier = TableIdentifier.of(namespace, ICEBERG_TABLE);
                final var table = catalog.createTable(tableIdentifier, schema, partitionConfig);
                final var filePath = table.location() + "/" + UUID.randomUUID().toString();
    
                final var file = table.io().newOutputFile(filePath);
                DataWriter<org.apache.iceberg.data.GenericRecord> dataWriter = Parquet.writeData(file)
                    .schema(schema)
                    .createWriterFunc(messageType -> GenericParquetWriter.create(schema, messageType))
                    .overwrite()
                    .withSpec(partitionConfig)
                    .build();
                        
                var movieRecord = org.apache.iceberg.data.GenericRecord.create(schema);
                int count = 0;
                for (var movie: movies) {
                    movieRecord = movieRecord.copy();
                    movieRecord.setField(MovieService.ID_FIELD, movie.getId());
                    movieRecord.setField(MovieService.NAME_FIELD, movie.getName());
                    movieRecord.setField(MovieService.RATING_FIELD, movie.getRating());
                    movieRecord.setField(MovieService.RELEASE_DATE_FIELD, movie.getReleaseDate());
                    dataWriter.write(movieRecord);
                    count++;
                }
                dataWriter.close();
   
                table.newAppend().appendFile(dataWriter.toDataFile()).commit();
                log.info("Successfully wrote {} movies to Iceberg table", count);
                return count;
            } catch (Exception ex) {
                throw new RuntimeException("Error writing to Iceberg", ex);
            }
        }

        // list tables from the namespace
        // List<TableIdentifier> tables = catalog.listTables(namespace);
        // CloseableIterable<Record> result = IcebergGenerics.read(table).where(Expressions.equal("level", "error"))
    }

    public List<MovieEntity> readMoviesFromIceberg() {
        try (JdbcCatalog catalog = new JdbcCatalog()) {
            final var props = new HashMap<String, String>();

            props.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
            props.put(CatalogProperties.URI, getJdbcCatalogUrl());
            props.put(JdbcCatalog.PROPERTY_PREFIX + "user", getJdbcCatalogUsername());
            props.put(JdbcCatalog.PROPERTY_PREFIX + "password", getJdbcCatalogPassword());
    
            props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + WAREHOUSE_BUCKET);
            props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
            props.put(S3FileIOProperties.ENDPOINT, s3EndpointOverride);
            props.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
            props.put(S3FileIOProperties.ACCESS_KEY_ID, "test");
            props.put(S3FileIOProperties.SECRET_ACCESS_KEY, "test");
            System.setProperty("aws.region", "us-east-1");

            catalog.setConf(new Configuration());
            catalog.initialize("MoviesCatalog", props);
    
            final var namespace = Namespace.of("public");
            final var tableIdentifier = TableIdentifier.of(namespace, ICEBERG_TABLE);
            final var table = catalog.loadTable(tableIdentifier);

            final var movies = new ArrayList<MovieEntity>();
            final var results = IcebergGenerics.read(table).build();
            for (var movieRecord : results) {
                final var movie = new MovieEntity();
                movie.setId(movieRecord.get(0, Long.class));
                movie.setName(movieRecord.get(1, String.class));
                movie.setRating(movieRecord.get(2, Float.class));
                movie.setReleaseDate(movieRecord.get(3, LocalDate.class));
                movies.add(movie);
            }
            results.close();
            return movies;

        } catch (Exception ex) {
            throw new RuntimeException("Error Reading from Iceberg", ex);
        }         
    }

}
