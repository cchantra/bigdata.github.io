from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer

ACCESS_TOKEN = '720187254216339456-jxjDSZi27GGQMNoVguO2UqFJyXWrcu6'
ACCESS_SECRET = 'z0nU30GEUloCgyc7XsyMPugQmTz4aL9XEpfPHg6EQEvZd'
CONSUMER_KEY = 'WDRZjnqvNG1msTvPHKlmFniYF'
CONSUMER_SECRET = '4NaUyVGG1k1xfFTjtwv1P8rx953pFZegenMsr8LASgEwVHDIqm'

kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
class StdOutListener(StreamListener):
    def on_data(self, data):
        kafka_producer.send("trump", data.encode('utf-8')).get(timeout=10)
        print (data)
        return True
    def on_error(self, status):
        print (status)



l = StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
stream = Stream(auth, l)
stream.filter(track=["trump"])
