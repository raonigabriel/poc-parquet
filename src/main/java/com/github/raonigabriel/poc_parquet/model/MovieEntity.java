package com.github.raonigabriel.poc_parquet.model;

import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table("movies")
public class MovieEntity implements Persistable<Long> {
    
    @Id
    private Long id;

    private String name;

    private Float rating;

    private LocalDate releaseDate;

    @Override
    public boolean isNew() {
        return id == null;
    }

}
