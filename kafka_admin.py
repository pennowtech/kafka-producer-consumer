import logging
from confluent_kafka.admin import AdminClient, NewPartitions

"""
Reference:
    https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
"""


class KafkaAdmin:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__client = AdminClient({'bootstrap.servers': '192.168.1.42:9092'})

    def create_partitions(self, num_partitions, topic_name):
        # Create Admin client
        self.list_kafka_info()

        new_parts = [NewPartitions(topic_name, int(num_partitions))]

        # Try switching validate_only to True to only validate the operation
        # on the broker but not actually perform it.
        fs = self.__client.create_partitions(new_parts, validate_only=False)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Additional partitions created for topic {}".format(topic))
            except Exception as e:
                print("Failed to add partitions to topic {}: {}".format(topic, e))

    def list_kafka_info(self):
        """ list topics, groups and cluster metadata """

        what = "all"

        md = self.__client.list_topics(timeout=10)

        print("Cluster {} metadata (response from broker {}):".format(
            md.cluster_id, md.orig_broker_name))

        if what in ("all", "brokers"):
            print(" {} brokers:".format(len(md.brokers)))
            for b in iter(md.brokers.values()):
                if b.id == md.controller_id:
                    print("  {}  (controller)".format(b))
                else:
                    print("  {}".format(b))

        if what in ("all", "topics"):
            print(" {} topics:".format(len(md.topics)))
            for t in iter(md.topics.values()):
                if t.error is not None:
                    errstr = ": {}".format(t.error)
                else:
                    errstr = ""

                print("  \"{}\" with {} partition(s){}".format(
                    t, len(t.partitions), errstr))

                for p in iter(t.partitions.values()):
                    if p.error is not None:
                        errstr = ": {}".format(p.error)
                    else:
                        errstr = ""

                    print("partition {} leader: {}, replicas: {},"
                          " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
                                                        p.isrs, errstr))
