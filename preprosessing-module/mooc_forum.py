import os
import csv
import pymongo
import json
import time
import datetime
import numpy as np

db_name = 'NarrativeMOOC'
filepath = '/Users/lizhen/data/vismooc'
client = pymongo.MongoClient('localhost', 27017)

def parse_date(d):
    if type(d) is int:
        return d
    else:
        return time.mktime(datetime.datetime.strptime(d[0: -5], '%Y-%m-%dT%H:%M:%S').timetuple())

courses = (f'{filepath}/{x}' for x in os.listdir(filepath) if os.path.isdir(f'{filepath}/{x}'))

for course in courses:
    print(f'current course is {course}.')
    db_name = 'NarrativeMOOC' + course.split('/')[-1]
    db = client[db_name]

    data_dirs = (f'{course}/databaseData/{x}' for x in os.listdir(f'{course}/databaseData/')
                 if os.path.isdir(f'{course}/databaseData/{x}') and 'ignore' not in x)

    db.forumthreads.drop()
    db.forumthreads.create_index([('id', pymongo.HASHED)])
    db.forumthreads.create_index([('created', pymongo.ASCENDING)])
    for data_dir in data_dirs:
        data_files = (x for x in os.listdir(data_dir)
                      if '.mongo' in x and 'prod' in x)
        for data_file in data_files:
            counter = 0
            print(f'processing {data_dir}/{data_file}')
            counter = 0
            titles = {}
            bodys = {}
            with open(f'{data_dir}/{data_file}', 'r') as jsonin:
                for row in jsonin:
                    thread = json.loads(row)
                    if counter % 100 == 0:
                        print(f'{counter} threads pre-processed...', end='\r')
                    _id = thread.get('_id', {}).get('$oid', '')
                    titles[_id] = thread.get('title', '')
                    bodys[_id] = thread.get('body', '')

            with open(f'{data_dir}/{data_file}', 'r') as jsonin:
                for row in jsonin:
                    thread = json.loads(row)
                    votes = thread.get('votes', {})
                    _id = thread.get('_id', {}).get('$oid', '')
                    pid = thread.get('comment_thread_id', {}).get('$oid', '')
                    item = {
                        '_type': thread.get('_type', ''),
                        'type': thread.get('thread_type', ''),
                        'id': _id if pid == '' else pid,
                        'anonymous': thread.get('anonymous', ''),
                        'anonymous_to_peers': thread.get('anonymous_to_peers', ''),
                        'user_id': thread.get('author_id', ''),
                        'username': thread.get('author_username', ''),
                        'commentable_id': str(thread.get('commentable_id', 0)),
                        'created': int(parse_date(thread.get('created_at', {}).get('$date', 0))),
                        'updated': int(parse_date(thread.get('updated_at', {}).get('$date', 0))),
                        'modified': int(parse_date(thread.get('last_activity_at', {}).get('$date', 0))),
                        'comment_count': thread.get('comment_count', 0),
                        'title': titles.get(pid) if pid != '' else thread.get('title', ''),
                        '_body': bodys.get(pid) if pid != '' else '',
                        'floor1': False if pid != '' else True,
                        'body': thread.get('body', ''),
                        'visible': thread.get('visible', False),
                        'count': votes.get('count', 0),
                        'up_count': votes.get('up_count', 0),
                        'down_count': votes.get('down_count', 0),
                    }
                    db.forumthreads.insert_one(item)
                    if counter % 100 == 0:
                        print(f'{counter} threads have been inserted into database...', end='\r')
                    counter += 1