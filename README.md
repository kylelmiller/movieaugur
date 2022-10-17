# MovieAugur

MovieAugur is an asset recommendation system that is a fully distributed microservice with high scalability and 
elasticity. It currently collects tvseries and movie data and provides popularity and recommendations on those assets.
It designed in a generic way that enables easily adding other data sources (books, chairs, etc.).

The system supports the following use cases:
* Popularity
* Recommendations

To support these use cases there are:
* Airlow scheduled offline jobs which:
  * Collect metadata
  * Collect popularity data
  * Collect user interaction data
  * Builds a recommendation model
* Event Service
  * Http endpoint which accepts user interaction data. This flows into the recommendation model building job.
* NGINX API Gateway
  * Acts as a reverse proxy so there is a single endpoint that is hit for all requests. The requests are then routed
    to the appropriate services.
* Kafka
  * Allows for asyncronous communication between the offline jobs and the services
* Kafka Connect
  * Used to copy data from Kafka to the services data stores (MongoDB, Redis) 

What is not here:
* User authentication
* Observability/Metrics
* More like this
  * Given an item id and object type return a list of item ids that are similar to that item
  * This could be added with:
    * New MLT service
    * ElasticSearch backend
    * Kafka connect sink from the item metadata topic
* User Interaction History
  * What has the user interacted with recently. Could be used to filter out results from other services.
  * This could be added with:
    * New interaction history service
    * Postgres backend
    * Kafka connect sink from user interactions topic
* Content filtering recommendation model
  * Another way to recommend items. Instead of relying on a collabrative model, this relies on a user's past consumption
    and the item's metadata. This is useful in recommending cold start items that the colab model will struggle with.
  * This could be added with:
    * New DAG
    * New offline model that pulls from the item metadata topic and the user interaction topic and uses xgboost to
      provide recommendations. The results could be writen to a new topic.
    * Kafka connect which copies data from the topic to Redis.
    * New content filtering service which returns results stored in Redis.
* User Lists
  * Keeping track of a user's indicated future interests. Like indicating future shows they want to watch or shows they 
    want to be informed there are new episodes for.
  * This could be added with:
    * New user lists service
    * Postgres backend
* New Arrival Event System
  * If there is a new item that shows up either through the popularity api or it starts to get user interactions we
    could send an email to users we think or know would like that item. This could be performed by using the item lists
    or the content filtering model. You would need registered users with contact information.
  * This could be added with: 
    * New DAG
    * New offline job which would look to see if there was a new item and then determine if any user would really like
      it, has not currently interacted with the item and hasn't been contact about this in the past.
* User Cold Start
  * You can do a simple user cold start by using the popularity endpoints and the user interaction endpoints. There
    are better ways to survey a new user's interests. There could be a service which asks for feedback on a wide range
    of items and narrows down the user's likes based on other user's interests.
  * This could be added with:
    * New DAG
    * New offline model
    * New service
    * Kafka connect sink to Redis
