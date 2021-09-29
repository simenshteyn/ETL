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