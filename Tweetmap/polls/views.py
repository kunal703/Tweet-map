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
from django.views.decorators.csrf import csrf_exempt, csrf_protect

consumer_key = 'YOUR_CONSUMER_KEY'
consumer_secret = 'YOUR_CONSUMER_SECRET'
access_token = 'YOUR_ACCESS_TOKEN'
access_secret = 'YOUR_ACCESS_SECRET'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)


es = Elasticsearch()

class StreamListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def __init__(self, time_limit=10):
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


#streamer = tweepy.Stream(auth=auth, listener=StreamListener())
@csrf_protect
def filter(request):
    if request.method == "POST":
        #print(request.POST)
        if 'searchname' in request.POST:
            query = str(request.POST.get('searchname', ''))
            #print(query)
            try:
                l = StreamListener(time_limit=10)
                auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_token, access_secret)
                stream = tweepy.Stream(auth, l)
                stream.filter(track=[query])

                if not os.path.exists('my_file_1'):
                    return render(request, 'map.html', {
                        "mydata": []
                    })
                res = file_read()
                tweets = []
                #print(res)
                for i in res:
                    tweets.append(es.get(index="idx_twp", doc_type='tweet', id=i))
                #print(tweets)
                pass_list = json.dumps(tweets)
                print(pass_list)
                if os.path.exists('my_file_1'):
                    os.remove('my_file_1')
                return render(request, 'map.html', {
                    "mydata" : pass_list
                })
            except (AttributeError, ValueError) as v:
                print(v)
                return HttpResponse('Error')


#Renders the index.html on startup
@csrf_exempt
def init_index(request):
    return render(request, 'index.html')


#Read the file consisting of tweets  and index it using elasticsearch
def file_read():
    with open('my_file_1') as f:
        result = []
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
              #  print(data)
                es.index(index="idx_twp", doc_type='tweet', id=data.get('id'), body=doc)
                result.append(data.get('id'))
            except:
                #print('Error Here')
                pass
    return result
