package com.github.raonigabriel.poc_parquet.repository;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.stereotype.Repository;

import com.github.raonigabriel.poc_parquet.model.MovieEntity;

@Repository
public interface MovieRepository extends ListCrudRepository<MovieEntity, Long> {

    @Modifying
    @Query("DELETE FROM movies WHERE id > :id")
    void deleteAllByIdGreaterThan(Long id);

}
