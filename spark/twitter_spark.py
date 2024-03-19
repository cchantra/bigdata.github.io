#!/usr/bin/env python
# coding: utf-8

# In[9]:


import socket
import sys
import requests
import requests_oauthlib
import json

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener



# Replace the values below with yours
ACCESS_TOKEN = '720187254216339456-jxjDSZi27GGQMNoVguO2UqFJyXWrcu6'
ACCESS_SECRET = 'z0nU30GEUloCgyc7XsyMPugQmTz4aL9XEpfPHg6EQEvZd'
CONSUMER_KEY = 'WDRZjnqvNG1msTvPHKlmFniYF'
CONSUMER_SECRET = '4NaUyVGG1k1xfFTjtwv1P8rx953pFZegenMsr8LASgEwVHDIqm'


class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      try:
          msg = json.loads( data )
          #print(msg['text'].encode('utf-8'))
          print( msg['text'].encode('utf-8') ) # Print the message and UTF-8 coding will eliminate emojis
          ## self.client_socket.send( msg['text'].encode('utf-8') )  this line is wrong , add the "\n" 
          self.client_socket.send((str(msg['text']) + "\n").encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
      print(status)
      return True







def sendData(c_socket):
  auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['DataScience','python','Bigdata','Iot'])




if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
    host = "127.0.0.1"     # Get local machine name
    port = 9998                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port
    
    print("Listening on port: %s" % str(port))
    s.listen(1)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.
    print("Received request from: " + str(addr))
    print(c)
    sendData(c)

