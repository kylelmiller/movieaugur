# Batch Jobs

We need to get data into the system before we can actually 
do anything useful. I'm using Airflow and some batch jobs to pull 
movielens reviews as some simple user feedback and metadata from tmdb.
Movielens has id mappings to tmdb ids. The data is pulled and then 
published to kafka so it can be picked up by downstream services.