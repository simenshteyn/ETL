-- Schema for content. Diagram at: https://dbdiagram.io/d/612718116dc2bb6073bbe779
CREATE SCHEMA IF NOT EXISTS content;

CREATE TYPE content.person_role AS ENUM (
    'actor',
    'director',
    'writer'
);

CREATE TYPE content.movie_type AS ENUM (
    'movie',
    'serial'
);

CREATE TABLE IF NOT EXISTS content.movies (
    movie_id        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    movie_title     text        NOT NULL,
    movie_desc      text,
    movie_rating    numeric(2, 1)
                    CHECK (movie_rating BETWEEN 0 AND 10),
    movie_type      content.movie_type
                                NOT NULL DEFAULT 'movie',
    created_at      timestamp with time zone DEFAULT (now()),
    updated_at      timestamp with time zone DEFAULT (now()),
    UNIQUE (movie_title, movie_rating)
);

CREATE TABLE IF NOT EXISTS content.people (
    person_id       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name       text        NOT NULL,
    person_desc     text,
    birthday        date,
    created_at      timestamp with time zone DEFAULT (now()),
    updated_at      timestamp with time zone DEFAULT (now()),
    UNIQUE (full_name, birthday)
);

CREATE TABLE IF NOT EXISTS content.genres (
    genre_id        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    genre_name      text        UNIQUE NOT NULL,
    genre_desc      text,
    created_at      timestamp with time zone DEFAULT (now()),
    updated_at      timestamp with time zone DEFAULT (now())
);

CREATE TABLE IF NOT EXISTS content.movie_people (
    movie_people_id uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    movie_id        uuid        NOT NULL,
    person_id       uuid        NOT NULL,
    person_role     content.person_role
                                NOT NULL,
     UNIQUE (movie_id, person_id, person_role),
    FOREIGN KEY (movie_id)
            REFERENCES content.movies
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    FOREIGN KEY (person_id)
            REFERENCES content.people
            ON DELETE CASCADE
            ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS content.movie_genres (
    movie_genres_id uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    movie_id        uuid        NOT NULL,
    genre_id        uuid        NOT NULL,
     UNIQUE (movie_id, genre_id),
    FOREIGN KEY (movie_id)
            REFERENCES content.movies
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    FOREIGN KEY (genre_id)
            REFERENCES content.genres
            ON DELETE CASCADE
            ON UPDATE CASCADE
);

CREATE INDEX ON content.movies(movie_title);

CREATE INDEX ON content.people(full_name);

-- Triggers to handle update logic. When genre_name or any person full_name
-- is updated, it automatically sets new updated_at field for movies with
-- these genres and people.

-- Function to handle updates at genres table.
CREATE OR REPLACE FUNCTION function_genres_updated() RETURNS TRIGGER AS
$BODY$
BEGIN
    UPDATE content.movies
       SET updated_at = NEW.updated_at
     WHERE movie_id IN (SELECT movie_id
                            FROM content.movie_genres AS mg
                           WHERE mg.genre_id = NEW.genre_id);
    RETURN new;
END;
$BODY$
language plpgsql;

-- Trigger to handle updates at genres table.
CREATE TRIGGER genres_updated
    AFTER UPDATE OF genre_name ON content.genres
    FOR EACH ROW
    EXECUTE PROCEDURE function_genres_updated();

-- Function to handle updates at people table.
CREATE OR REPLACE FUNCTION function_people_updated() RETURNS TRIGGER AS
$BODY$
BEGIN
    UPDATE content.movies
       SET updated_at = NEW.updated_at
     WHERE movie_id IN (SELECT movie_id
                            FROM content.movie_people AS mp
                           WHERE mp.person_id = NEW.person_id);
    RETURN new;
END;
$BODY$
language plpgsql;

-- Trigger to handle updates at people table.
CREATE TRIGGER people_updated
    AFTER UPDATE OF full_name ON content.people
    FOR EACH ROW
    EXECUTE PROCEDURE function_people_updated();