# from multiprocessing.dummy import Pool as ThreadPool
import json
import boto3
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
#from geocoder import location
from textblob import TextBlob
from requests_aws4auth import AWS4Auth

aws_access_key_id = 'AKIAIKLXNT7QJ3F2A5JQ'
aws_secret_access_key = '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'

sqs = boto3.resource('sqs',region_name='us-east-2',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
q = sqs.get_queue_by_name(QueueName='sqs')

snsClient = boto3.client('sns',region_name="us-east-2",aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)



host = 'search-tweet-pmdufytncggnxaiqxxdluladdq.us-east-2.es.amazonaws.com'
awsauth = AWS4Auth(aws_access_key_id,aws_secret_access_key,'us-east-2', 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def getSQSQueue():
    try:
    # code to get data from sqs
      for message in q.receive_messages(MessageAttributeNames=['location']):
        tweet = message.body
        tweet=json.loads(tweet)
        print tweet
        tweet_text= tweet["tweets"]
        text1=tweet_text.lower().encode('ascii','ignore').decode('ascii')
        blob = TextBlob(text1)
        # sentiment = blob.sentiment.polarity
        # print sentiment
        if blob.sentiment.polarity > 0:
            sentiment= "positive"
        elif blob.sentiment.polarity == 0:
            sentiment= "neutral"
        else:
            sentiment= "negative"
        print sentiment
        # es.indices.delete(index='cloud_tweet', ignore=[400, 404])
        tweet["sentiment"]=sentiment
                      
        response = snsClient.publish(TopicArn='arn:aws:sns:us-east-2:500763754709:Tweet',
                                     Message=json.dumps({
         										'content':tweet["tweets"],
         										'location':tweet["location"],
         										'sentiment':sentiment
        									 }),
                                     Subject='Indexing'
                                     )
        print response
        return tweet
    except Exception as e:
    	print e
        pass

for n in range(1000):
    tweet_text=getSQSQueue()




# def calculateParallel(numbers, threads):
#     # configure the worker pool

#     pool = ThreadPool(processes= 1)
#     results = pool.map(getSQSQueue,numbers)
#     pool.close()
#     pool.join()
#     return results




# if __name__ == "__main__":
#     numbers = [1, 2, 3, 4, 5, 6]

#     for n in range(50):
#         tweet_text = calculateParallel(numbers, 10)

#         print(n)
