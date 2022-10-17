# Batch Jobs
We need to get data into the system before we can actually 
do anything useful. I'm using Airflow and some batch jobs to pull data
from external sources and publishing it to Kafka.

## Jobs
### MovieLens 100k
This job collects the MovieLens 100k dataset. It converts all of the
MovieLens ids into TMDB ids, collects the TMDB metadata for all
of the movies in the 100k dataset, sends the metadata to Kafka and 
sends the movie/user interactions to Kafka as well.

This job isn't scheduled. It's just run manually once to get the
interaction and metadata into the system.

### TMDB Popular Movies
This job hits the TMDB popular movies api, gets the list of movie
ids, collects the TMDB metadata for those ids and sends the metadata
to Kafka.

Runs daily to pull the latest popular movies.

### TMDB Popular Series
This job hits the TMDB popular tv api, gets the list of tv series
ids, collects the TMDB metadata for those ids and sends the metadata
to Kafka.

Runs daily to pull the latest popular tv series.

### Build Recommendation Model
Reads from the user interactions Kafka topic, creates a recommendation model and then writes the results to Kafka. I
went with iALS based on the results in the paper: [Revisiting the Performance of iALS on Item Recommendation Benchmarks](https://arxiv.org/abs/2110.14037).
It's fast and the results are competitive. The algorithm is completely decoupled from the service so if SotA does start
to pull away it's easy to swap out. The results are written to Kafka and the eventual consuming service just reads the 
recommendation results from Redis.

It would be better if all of the input data wasn't stored in Kafka and instead in s3 or some other distributed storage
but this is fine for now.