"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        
        self.broker_properties = {"bootstrap.servers": BROKER_URL,
                                  "schema.registry.url": SCHEMA_REGISTRY_URL}        
        

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        schema_registry =  CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})        
        self.producer = AvroProducer({"bootstrap.servers": BROKER_URL},
                                     schema_registry=schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # Creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        logger.info("beginning topic creation for %s", self.topic_name)
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        topic = NewTopic(self.topic_name, num_partitions=self.num_partitions,
                         replication_factor=self.num_replicas)
        
        client.create_topics([topic])        
        logger.info("creating topic")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""        
        #cleanup the Producer   
        logger.info("cleaning up the producer")
        self.producer.flush()        

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
