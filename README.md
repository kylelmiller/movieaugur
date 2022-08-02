# MovieAugur

MovieAugur is an asset recommendation system that is a fully distributed
microservice with high scalability and elasticity.

* User Interaction Events
* MLT (ES)
* Recommendations (SVD - https://github.com/NicolasHug/Surprise)
* Popularity (Redis)
* Content Model (XGBoost)
* Kafka
* Airflow 
  * Gets metadata
  * Gets user interactions
  * Builds recommendation model
  * Builds content model
  * Builds popularity model