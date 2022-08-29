# Popularity Service

This is a simple service. It sets up Kafka connect to write from the popularity
topic to Redis, reads from Redis, gets the items metadata from the metadata service 
and returns an ordered list of the most popular content. Right now it is powered by 
the TMDB popular movie/series endpoints. That data enters the system through 
two offline jobs. In the future, it's going to be powered through an event service. 