from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import RuntimeContext, SinkFunction

class CassandraCheckSink(SinkFunction):
    """Example sink that performs a Cassandra lookup once per unified event."""

    def __init__(self, contact_points, keyspace):
        self.contact_points = contact_points
        self.keyspace = keyspace
        self.session = None

    def open(self, runtime_context: RuntimeContext):
        from cassandra.cluster import Cluster
        cluster = Cluster(self.contact_points)
        self.session = cluster.connect(self.keyspace)

    def invoke(self, value, context):
        # Example query; adjust to your schema
        self.session.execute("SELECT * FROM my_table WHERE id=%s", (value['id'],))

    def close(self):
        if self.session:
            self.session.shutdown()

def build_kafka_consumer(topic, kafka_props):
    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )

def run(topics, kafka_props, cassandra_contact_points, cassandra_keyspace):
    env = StreamExecutionEnvironment.get_execution_environment()

    streams = [env.add_source(build_kafka_consumer(t, kafka_props)) for t in topics]
    if not streams:
        raise ValueError("No Kafka topics provided")

    unified_stream = streams[0]
    for s in streams[1:]:
        unified_stream = unified_stream.union(s)

    # Example: convert JSON string to dict assuming each record is JSON
    from pyflink.datastream.functions import MapFunction
    import json

    class JsonParser(MapFunction):
        def map(self, value):
            return json.loads(value)

    parsed_stream = unified_stream.map(JsonParser())

    parsed_stream.add_sink(CassandraCheckSink(cassandra_contact_points, cassandra_keyspace))

    env.execute("Kafka Topic Union to Cassandra")

if __name__ == "__main__":
    # Example usage
    kafka_properties = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-consumer'
    }
    TOPICS = [
        'clickstream_topic',
        'pageview_topic',
        'wikidocument_topic'
    ]
    run(TOPICS, kafka_properties, ['cassandra'], 'wiki_keyspace')
