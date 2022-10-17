# Recommendation Service

This is a simple service. It sets up Kafka connect to write from the recommendations
topic to Redis, reads from Redis, gets the items metadata from the metadata service 
and returns an ordered list of the user's recommended content. The user interactions come from movielens
and the user event service. 
