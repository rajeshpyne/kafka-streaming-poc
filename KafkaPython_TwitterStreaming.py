import json
import sys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "********"
access_token_secret =  "******"
consumer_key =  "*****"
consumer_secret =  "**********"

kafka_topic = "<custom topic of kafka>"

keyword = ["List","Of","Keyword","Keyword1","",""]

class StdOutListener(StreamListener):
        def on_data(self, data):
                producer.send_messages(kafka_topic, data.encode('utf-8'))
                print (data)
                return True
        def on_error(self, status):
                print (status)

kafka = KafkaClient("<ip of machine>:<port of kafka>")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
streamer = Stream(auth, l)
streamer.filter(track=keyword)