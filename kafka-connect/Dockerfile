FROM confluentinc/cp-kafka-connect:5.5.11

RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.7.0 \
    && confluent-hub install --no-prompt jcustenborder/kafka-connect-redis:0.0.2.17