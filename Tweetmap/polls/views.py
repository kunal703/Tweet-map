from django.shortcuts import render
import tweepy
import sys
import json
from textwrap import TextWrapper
from datetime import datetime
from elasticsearch import Elasticsearch
import time
import os
from decimal import*
from django.http import HttpResponse, JsonResponse
from django.template import RequestContext
import re
from rest_framework.authentication import SessionAuthentication



consumer_key = 'f0y6JU1MAxBeCx2tQihj7aGfq'
consumer_secret = 'IKw3W8LIpT1dmKIIErai7cjRttFndBKgWqBIPXQJ3wV20WgZ4w'
access_token = '3287298026-lcRtdp82KxjWyAXswwQGrXyVhCyZKRltQvB7XAI'
access_secret = 'jS94pRtjZJ55E05ZYbCBfXpLZLx5uGRjQBbs9aCTJT8cr'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)


es = Elasticsearch()

class StreamListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
    #counttweets = 0
    if os.path.exists('my_file_1'):
        os.remove('my_file_1')

    def __init__(self, time_limit=20):
        self.start_time = time.time()
        self.limit = time_limit
        super(StreamListener, self).__init__()

    def on_status(self, status):
        if (time.time() - self.start_time) < self.limit:
            #print 'n%s %s' % (status.author.screen_name, status.created_at)
                tweets = status._json
                if (tweets['place']):
                    print(tweets['text'])
                    self.append_record(tweets)
                    return True
                else:
                    pass
        else:
            return False

    def on_error(self, status_code):
        print("response: %s" % status_code)
        if status_code == 420:
            return False

    def append_record(self, record):
        with open('my_file_1', 'a') as f:
            json.dump(record, f)
            f.write(os.linesep)
'''
streamer = tweepy.Stream(auth=auth, listener=StreamListener() ,timeout=5)

#Fill with your own Keywords bellow
streamer.filter(track=['a'])
#streamer.userstream(None)
print("hi")
with open('my_file_1') as f:
    for line in f:
        try:
            data = json.loads(line)
            place = data.get('place')
            #print(place)
            coordinates = (place.get('bounding_box').get('coordinates')[0])[0]
            lat = coordinates[0]
            lng = coordinates[1]
            #print(lat)
            #print(lng)
            lng = Decimal(("%0.5f" % lng))
            lat = Decimal(("%0.5f" % lat))
            doc = {
                "timestamp": datetime.now(),
                "location": {
                    "lat": lat,
                    "lon": lng
                },
                "title": data.get('text')
            }
            es.index(index="idx_twp", doc_type='tweet', id=data.get('id'), body=doc)
            # pprint(data)
            res=es.get(index="idx_twp", doc_type='tweet', id=data.get('id'))
            print(res)

        except:
            print
            ('Error here')
            pass
'''
def filter(request):
    if request.method == "GET":
        if 'searchstring' in request.GET:
            query = str(request.GET.get('searchstring', ''))
            try:
                l = StreamListener(time_limit=20)
                auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_token, access_secret)
                stream = tweepy.Stream(auth, l)
                stream.filter(track=[query])
                res=file_read()
                tweets=es.search(index='tweet_index', body={"from" : 0, "size" : 1000,"query":{"match_all":{}}})
                tweets=tweets['hits']['hits']

                b=[]
                for tweet in tweets:
                    source = tweet.get('_source')
                    a={}
                    #a.append(source.get('location').get('lat'))
                    #a.append(source.get('location').get('lon'))
                    #a.append(source.get('title'))
                    a['lat'] = source.get('location').get('lat')
                    a['lon'] = source.get('location').get('lon')
                    a['title'] = re.escape(source.get('title'))
                    b.append(a)
                if os.path.exists('my_file_1'):
                    os.remove('my_file_1')
                return JsonResponse({'tweets': b})
            except (AttributeError, ValueError) as v:
                print (v)
                return HttpResponse('Error')


#Renders the index.html on startup
def init_index(request):
    return render(request, 'index.html')


#Read the file consisting of tweets  and index it using elasticsearch
def file_read():
    with open('my_file_1') as f:
        for line in f:
            try:
                data = json.loads(line)
                place = data.get('place')
                coordinates = (place.get('bounding_box').get('coordinates')[0])[0]
                lat = coordinates[0]
                lng = coordinates[1]
                lng = Decimal(("%0.5f" % lng))
                lat = Decimal(("%0.5f" % lat))
                doc = {
                "timestamp":datetime.now(),
                "location":{
                    "lat":lng,
                    "lon":lat
                } ,
                "title":data.get('text')
                }
                #pprint(data)
                res = es.index(index="tweet_index", doc_type='tweet', id=data.get('id'), body=doc)
            except:
                print ('Error here')
                pass
    return res