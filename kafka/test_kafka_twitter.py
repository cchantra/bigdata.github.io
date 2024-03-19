from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer

ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

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
