"""
For generic interaction with a kafka stream.
"""

from pykafka import KafkaClient


try:
    import ujson as json #Faster for some operations
except:
    import json

class TrawlerKafka:
    def __init__(self, host, port, topic='trawler'):
        """
        Connect to a running Kafka instance on `host` and
        `port`, and return closures around this to easily
        push tweets to it.
        """

        kafka_client = KafkaClient(hosts="%s:%s" % (host,port))
        topic = kafka_client.topics[topic]
        producer = topic.get_producer()
        self.client = kafka_client
        self.topic = topic
        self.producer = producer
        """self.consumer = topic.get_simple_consumer(consumer_group='mygroup',
                                                  #auto_offset_reset=OffsetType.EARLIEST,
                                                  reset_offset_on_start=False)
                                                  """
        #self.consumer.start()
        #self.consumer.
        self.consumer = topic.get_simple_consumer()
        
    def send_individual_tweets( self, tweets):
        """
        Send each tweet in `tweets` as an individual Kafka message. 
        """
        for tweet in tweets:
            self.producer.produce([json.dumps(tweet)])

    def send_individual_tweet( self, tweet):
        """
        Send this tweet as a message.
        """
        self.send_individual_tweets([tweet])


    def send_bulk_tweets( self, tweets):
        """
        Send `tweets` in bulk as a single kafka message.
        """
        self.producer.produce(json.dumps(tweets))

    def get_tweets(self):
        """
        Get and reconstitute tweets from a kafka queue
        """
        for message in self.consumer:
            if message is not None:
                tweet = json.loads(message.value)
                yield tweet

        
if __name__ == '__main__':
    """
    Run standalone to test
    """
    
    trawler_kafka = TrawlerKafka( 'localhost',9092)
    import datetime as dt
    tweets = [{'text':'tweet tweet','timestamp':dt.datetime.now().isoformat()},{'text':'tweety tweet tweet'},{'text':'tweety tweet tweety tweet'}]
    for i in range(10):
        trawler_kafka.send_individual_tweets(tweets)

    print "------------------------"

    trawler_kafka2 = TrawlerKafka('localhost',9092)
    for m in trawler_kafka2.get_tweets():
        print m.keys()
