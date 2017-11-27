#Import the necessary methods from tweepy library
import boto3
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys  
import json
from datetime import datetime 
import time 

access_token = "919581457982640128-ullOyY52aA057rB3bBp0P0j7xBrEJFg"
access_token_secret = "7qnHxbaEE3NnG5XwAxC1IKGBZnPcuAWIWG6Gpl7LiOdpQ"
consumer_key = "ZSh6PfXZzVZl2iSrwpMXeiNkW"
consumer_secret = "sc7RXbO7LXocrdnuWfaube6VinvWx6HWKc0IxTvyEoLMnCf2xZ"
aws_access_key_id = 'AKIAIKLXNT7QJ3F2A5JQ'
aws_secret_access_key = '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'

# host = 'search-tweetmap-t5bf6hbxw46x5ryowaiemq5due.us-east-2.es.amazonaws.com'
#awsauth = AWS4Auth(aws_access_key_id,aws_secret_access_key,'us-east-2', 'es')

# es = Elasticsearch()
#     hosts=[{'host': host, 'port': 443}],
#     http_auth=awsauth,
#     use_ssl=True,
#     verify_certs=True,
#     connection_class=RequestsHttpConnection
# )


sqs = boto3.resource('sqs',region_name='us-east-2',
	aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
q = sqs.get_queue_by_name(QueueName='sqs')
print (q.url)
print(q.attributes.get('DelaySeconds'))

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_data = json.loads(data)
        if 'text' in json_data:
            tweets = json_data['text'].lower().encode('ascii','ignore').decode('ascii')
            location = json_data['user']['location']
            id = str(json_data['id'])
            lon = None
            lat = None
            if json_data['coordinates']:
                lat = float(json_data['coordinates']['coordinates'][0])
                lon = float(json_data['coordinates']['coordinates'][1])
            elif 'place' in json_data.keys() and json_data['place']:
                lat = float(json_data['place']['bounding_box']['coordinates'][0][0][0])
                lon = float(json_data['place']['bounding_box']['coordinates'][0][0][1])
            obj={"tweets": tweets, "location" : {"lat":lat, "lon":lon}}
            if lat and lon:
                try:
                    response = q.send_message(MessageBody=json.dumps(obj))
                except Exception as e:
                    print e
    def on_error(self, status):
        print status        

if __name__ == '__main__':

   
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    

    try:
        l = StdOutListener()
        stream = Stream(auth, l)
        terms = [
        'kohli', 'modi'
        ,'hollywood','bollywood', 'trump', 'them', 'this', 'india'
        ]
        stream.filter(track=terms)
    except Exception as e:
        raise e
    