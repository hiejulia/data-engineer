REGISTER 'udfs/movies_udf.py' USING streaming_python AS movies_udf;

-- Load the data from the file system
records = LOAD '../resources/movies' USING PigStorage('|') 
   AS (id:int, title:chararray, release_date:chararray);

-- Parse the titles and determine how many days since the release date
titles = FOREACH records GENERATE movies_udf.parse_title(title), movies_udf.days_since_release(release_date);

-- Order the movies by the time since release
most_recent = ORDER titles BY days_since_release ASC;

-- Get the ten most recent movies
top_ten = LIMIT most_recent 10;

-- Display the top ten most recent movies
DUMP top_ten;


-- $ pig -x local recent_movies.pig 
-- ...
-- (unknown,)
-- (Apt Pupil,6183)
-- (Mighty, The,6197)
-- (City of Angels,6386)
-- (Big One, The,6393)
-- (Lost in Space,6393)
-- (Mercury Rising,6393)
-- (Spanish Prisoner, The,6393)
-- (Hana-bi,6400)
-- (Object of My Affection, The,6400)