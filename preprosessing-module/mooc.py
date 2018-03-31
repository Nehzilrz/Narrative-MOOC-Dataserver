import os
import csv
import pymongo
import json
import time
import datetime
import pygeoip
import numpy as np
from peakutils.peak import indexes
import xml.etree.cElementTree as ET

gi = pygeoip.GeoIP('/Users/lizhen/GeoLiteCity.dat')

operation_time_timeout = 300
min_video_duration = 20
min_peak_size = 20
max_video_duration = 7200

db_name = 'NarrativeMOOC'
course_name = 'HKUSTx-EBA101x-1T2016' #'HKUSTx-COMP102x-2T2014'
filepath = '/Users/lizhen/data/vismooc'
client = pymongo.MongoClient('localhost', 27017)
users = {}
user_modules = {}
courseStructure = {}
categories = []
enable_events_insert = True
enable_activies_insert = True
peak_detection_types = ['pause_video', 'seek_forward', 'seek_backward']

entropy_scale = 10


def entropy(X, maxvalue = 1):
    Y = np.bincount(np.multiply(X, entropy_scale /
                                (maxvalue + 1e-4)).astype(int))
    probs = np.divide(Y[np.nonzero(Y)], len(X))
    return np.sum(-p * np.log2(p) for p in probs)


def value_freq(X, maxvalue = 1):
    Y = np.bincount(np.multiply(X, entropy_scale /
                                (maxvalue + 1e-4)).astype(int))
    freq = np.divide(Y, len(X))
    return freq.tolist()


def entropy_on_freq(X):
    probs = np.divide(X, sum(X))
    return np.sum(-p * np.log2(p) for p in probs)


def convertDateString(s):
    """[convert date string to timestamp]

    Arguments:
        s {[String]} -- [A Date String in Y-m-d H:M:S format.]

    Returns:
        [Number] -- [A timestamp in float]
    """

    if '.' in s:
        s = s.split('.')[0]
    return time.mktime(datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S').timetuple())


def parseUser(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
        counter = 0
        for row in tsvin:
            counter += 1
            if counter % 10000 == 0:
                print(f'{counter} rows have been loaded...', end='\r')
            if header == None:
                header = row
                header[header.index('id')] = 'user_id'
                uid = header.index('user_id')
            elif len(row) == len(header):
                if users.get(row[uid]) == None:
                    users[row[uid]] = {}
                user = users[row[uid]]
                for i, x in enumerate(row):
                    user[header[i]] = x
            else:
                print(row, len(row), len(header))
                continue
        print(f'{counter} rows have been loaded.', end='\r')


def parseUserProfile(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
        counter = 0
        for row in tsvin:
            counter += 1
            if counter % 10000 == 0:
                print(f'{counter} rows have been loaded...', end='\r')
            if header == None:
                header = row
                uid = header.index('user_id')
            elif len(row) == len(header):
                if users.get(row[uid]) == None:
                    users[row[uid]] = {}
                user = users[row[uid]]
                for i, x in enumerate(row):
                    user[header[i]] = x
            else:
                print(row, len(row), len(header))
                continue
        print(f'{counter} rows have been loaded.', end='\r')


def parseCertificate(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
        counter = 0
        for row in tsvin:
            counter += 1
            if counter % 10000 == 0:
                print(f'{counter} rows have been loaded...', end='\r')
            if header == None:
                header = row
                header[header.index('id')] = 'certificate_id'
                uid = header.index('user_id')
            elif len(row) == len(header):
                if users.get(row[uid]) == None:
                    users[row[uid]] = {}
                user = users[row[uid]]
                for i, x in enumerate(row):
                    user[header[i]] = x
            else:
                print(row, len(row), len(header))
                continue
        print(f'{counter} rows have been loaded.', end='\r')


def parseCoursewareStudentModule(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
        counter = 0
        for row in tsvin:
            counter += 1
            if counter % 10000 == 0:
                print(f'{counter} rows have been loaded...', end='\r')
            if header == None:
                header = row
                header[header.index('student_id')] = 'user_id'
                uid = header.index('user_id')
                mid = header.index('module_type')
            elif len(row) == len(header):
                if user_modules.get(row[mid]) == None:
                    user_modules[row[mid]] = {}
                if user_modules[row[mid]].get(row[uid]) == None:
                    user_modules[row[mid]][row[uid]] = []
                item = {}
                for i, x in enumerate(row):
                    item[header[i]] = x
                user_modules[row[mid]][row[uid]].append(item)
            else:
                print(row, len(row), len(header))
                continue
        print(f'{counter} rows have been loaded.', end='\r')


def parseStudentEnrollment(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
        counter = 0
        for row in tsvin:
            counter += 1
            if counter % 10000 == 0:
                print(f'{counter} rows have been loaded...', end='\r')
            if header == None:
                header = row
                header[header.index('id')] = 'enrollment_id'
                uid = header.index('user_id')
            elif len(row) == len(header):
                if users.get(row[uid]) == None:
                    users[row[uid]] = {}
                user = users[row[uid]]
                for i, x in enumerate(row):
                    user[header[i]] = x
            else:
                print(row, len(row), len(header))
                continue
        print(f'{counter} rows have been loaded.', end='\r')

def peak_detection2(vec):
    peaks = indexes(np.array(vec), thres = 0.1, min_dist = 5)
    ret = []
    for i in peaks:
        if i < 4 or i + 4 > len(vec):
            continue
        if np.std(vec[i - 2: i + 3]) == 0:
            continue
        ret.append({
            'start': int(i - 1),
            'end': int(i + 1),
            'length': 3,
            'signaficiant': (vec[i] - np.mean(vec[i - 2: i + 3])) / np.std(vec[i - 2: i + 3]),
        })
    return ret

def peak_detection(vec):
    pinLength = 1
    mean = np.mean(vec[0:5])
    std = abs(vec[0] - mean) + abs(vec[1] - mean) + \
        abs(vec[2] - mean) + abs(vec[3] - mean) + abs(vec[4] - mean)
    threshold = 3
    lag = 5
    start = 0
    end = 0
    diff = 0
    alpha = 0.125
    result = []
    vec_len = len(vec)

    for i in range(lag, vec_len - lag):
        if abs(vec[i] - mean) > threshold * std:
            start = i
            while i < vec_len and vec[i] > vec[i - 1]:
                diff = abs(mean - vec[i])
                mean = alpha * vec[i] + (1 - alpha) * mean
                std = alpha * diff + (1 - alpha) * std
                i += 1
            end = i
            while i < vec_len and vec[i] > vec[start]:
                diff = abs(mean - vec[i])
                mean = alpha * vec[i] + (1 - alpha) * mean
                std = alpha * diff + (1 - alpha) * std
                i += 1
                end = i
            result.append({
                'start': start,
                'end': end,
                'length': end - start + 1,
            })
        else:
            diff = abs(mean - vec[i])
            mean = alpha * vec[i] + (1 - alpha) * mean
            std = alpha * diff + (1 - alpha) * std

    return result


def parseUserIdMap(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
        counter = 0
        for row in tsvin:
            counter += 1
            if counter % 10000 == 0:
                print(f'{counter} rows have been loaded...', end='\r')
            if header == None:
                header = row
                header[header.index('id')] = 'user_id'
                uid = header.index('user_id')
            elif len(row) == len(header):
                if users.get(row[uid]) == None:
                    users[row[uid]] = {}
                user = users[row[uid]]
                for i, x in enumerate(row):
                    user[header[i]] = x
            else:
                print(row, len(row), len(header))
                break
        print(f'{counter} rows have been loaded.', end='\r')


def getOffspring(x):
    current = courseStructure[x]
    if current.get('offspring') != None:
        return current['offspring']

    current['offspring'] = []
    for y in current['children']:
        current['offspring'].append(y)
        courseStructure[y]['parent'] = x
        for z in getOffspring(y):
            current['offspring'].append(z)
    return current['offspring']


courses = (f'{filepath}/{x}' for x in os.listdir(filepath) if os.path.isdir(f'{filepath}/{x}'))

for course in courses:
    print(f'current course is {course}.')
    users = {}
    user_modules = {}

    db_name = 'NarrativeMOOC' + course.split('/')[-1]
    db = client[db_name]

    '''
    {
        "username": "Pontificator",
        "event_type": "seek_video",
        "ip": "202.126.199.60",
        "agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
        "host": "courses.edx.org",
        "session": "8796a796883119ff18778c8c22b51b8d",
        "event": {
            "id": "i4x-HKUSTx-COMP102x-video-4bbee764bc3c454e9d713c4671d76adc",
            "old_time": 284.92,
            "new_time": 237.88,
            "type": "onCaptionSeek",
            "code": "7ycpydl0gOc"
        },
        "event_source": "browser",
        "context": {
            "username": "Pontificator",
            "user_id": 1723033,
            "ip": "202.126.199.60",
            "org_id": "HKUSTx",
            "agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
            "host": "courses.edx.org",
            "session": "8796a796883119ff18778c8c22b51b8d",
            "course_id": "HKUSTx/COMP102x/2T2014",
            "path": "/event"
        },
        "time": "2014-08-07T03:25:40.605945+00:00",
        "page": "https://courses.edx.org/courses/HKUSTx/COMP102x/2T2014/courseware/0e9380db36894e7cbd23b50742464bf2/83c8429f431e4571b05f0c6f838b3a69/"
    }
    '''
    log_attributes = ['event_type', 'ip',
                      'session', 'event_source', 'time', 'page']
    log_event_attributes = ['id', 'currentTime',
                            'oldTime', 'newTime', 'code', 'type']
    log_context_attributes = ['course_id', 'user_id', 'path']

    has_tsv = False
    for x in (f'{course}/eventData/{x}' for x in os.listdir(f'{course}/eventData/') if x.endswith('.tsv')):
        has_tsv = True

    if not has_tsv:
        header = ['event_type', 'user_id', 'module_id', 'ip', 'session',
                  'event_source', 'time', 'current_time', 'new_time', 'code', 'type', 'path']
        log_files = (f'{course}/eventData/{x}' for x in os.listdir(f'{course}/eventData/') if x.endswith('.log'))
        for log_file in log_files:
            print(f'covert {log_file} to tsv format.')
            counter = 0
            tsv_file = log_file.replace('.log', '.tsv')
            tsvfile = open(tsv_file, 'w')
            writer = csv.writer(tsvfile, delimiter='\t')
            writer.writerow(header)
            with open(log_file, 'r') as records:
                logs = []
                for x in records:
                    counter += 1
                    x = json.loads(x)
                    logtime = time.mktime(datetime.datetime.strptime(
                        x.get('time').split('.')[0].split('+')[0], '%Y-%m-%dT%H:%M:%S').timetuple()
                    )
                    if x.get('event') == None:
                        continue
                    elif type(x.get('event')) == dict:
                        event = x.get('event')
                    elif type(x.get('event')) == str:
                        try:
                            event = json.loads(x.get('event'))
                        except:
                            continue
                    else:
                        continue
                    context = x.get('context')
                    if context.get('user_id') == None:
                        continue
                    if event.get('old_time') != None:
                        current_time = event.get('old_time')
                    else:
                        current_time = event.get('currentTime')
                    event_id = event.get('id')
                    if len(x.get('event_type')) > 20:
                        continue
                    log = [x.get('event_type'), context.get('user_id'), event_id, x.get('ip'),
                            x.get('session'), x.get(
                                'event_source'), logtime, current_time,
                            event.get('new_time'), event.get('code'), event.get('type'), context.get('path')]
                    writer.writerow(log)
                    if counter % 10000 == 0:
                        print(f'{counter} records have been processed...', end='\r')
                print(f'{counter} records have been processed.')

    tsv_files = (f'{course}/eventData/{x}' for x in os.listdir(f'{course}/eventData/') if x.endswith('.tsv'))

    sessions = {}
    event_types = {}

    if enable_events_insert:
        db.events.drop()
        db.events.create_index([('user_id', pymongo.HASHED)])
        db.events.create_index([('session', pymongo.HASHED)])
        db.events.create_index([('module_id', pymongo.HASHED)])
        db.events.create_index([('current_time', pymongo.ASCENDING)])
        db.events.create_index([('time', pymongo.ASCENDING)])

    counter = 0
    videos_duration = {}

    for tsv_file in tsv_files:
        print(f'loading {tsv_file}...')
        with open(tsv_file, 'r') as tsvin:
            tsvin = csv.reader(tsvin, delimiter='\t')
            first_line = True
            events = []
            for row in tsvin:
                counter += 1
                if counter % 10000 == 0:
                    print(f'{counter} records have been loaded...', end='\r')
                    if enable_events_insert:
                        db.events.insert_many(events)
                        events = []
                if first_line:
                    first_line = False
                    header = row
                    '''
                        ['event_type', 'user_id', 'module_id', 'ip', 'session', 'event_source', 'time', 'current_time', 'new_time', 'code', 'type', 'path']
                    '''
                    session_idx = header.index('session')
                    module_idx = header.index('module_id')
                    user_idx = header.index('user_id')
                    event_type_idx = header.index('event_type')
                    time_idx = header.index('time')
                    current_time_idx = header.index('current_time')
                    new_time_idx = header.index('new_time')
                    ip_idx = header.index('ip')
                else:
                    row[time_idx] = float(row[time_idx])
                    row[user_idx] = str(row[user_idx])
                    user_id = row[user_idx]
                    if users.get(user_id) == None:
                        users[user_id] = {}
                    user = users.get(user_id)
                    if user.get('country_code3') == None:
                        '''
                        {'dma_code': 0, 'area_code': 0, 'metro_code': None, 
                        'postal_code': None, 'country_code': 'CN', 'country_code3': 'CHN',
                         'country_name': 'China', 'continent': 'AS', 'region_code': '02', 
                         'city': 'Jiaxing', 'latitude': 30.752199999999988, 'longitude': 120.75,
                          'time_zone': 'Asia/Shanghai'}
                        '''
                        if row[ip_idx] != '':
                            x = gi.record_by_addr(row[ip_idx])
                            if x != None:
                                user['country_code3'] = x['country_code3']
                                user['country_name'] = x['country_name']
                                user['city'] = x['city']
                                user['continent'] = x['continent']
                    row[module_idx] = row[module_idx].replace(
                        'i4x-', 'i4x://').replace('-', '/').split('@')[-1]
                    row[current_time_idx] = float(row[current_time_idx] or 0)
                    row[new_time_idx] = float(row[new_time_idx] or 0)
                    if row[event_type_idx] == 'seek_video':
                        if row[current_time_idx] > row[new_time_idx]:
                            row[event_type_idx] = 'seek_forward'
                        else:
                            row[event_type_idx] = 'seek_backward'

                    if row[current_time_idx] > max_video_duration:
                        continue
                    elif videos_duration.get(row[module_idx]) == None or row[current_time_idx] > videos_duration.get(row[module_idx]):
                        videos_duration[row[module_idx]
                                        ] = int(row[current_time_idx]) + 1

                    if sessions.get(row[session_idx]) != None:
                        sessions[row[session_idx]].append(row)
                    else:
                        sessions[row[session_idx]] = [row]
                    if enable_events_insert:
                        if len(row) == len(header):
                            event = {}
                            for i, x in enumerate(row):
                                event[header[i]] = x
                            events.append(event)
            if len(events) > 0 and enable_events_insert:
                db.events.insert_many(events)
            print(f'{counter} records have been loaded.')

    user_events = {}
    video_events = {}
    video_users = {}
    counter = 0
    for session_name in sessions:
        counter += 1
        if counter % 1000 == 0:
            print(f'{counter} sessions have been processed...', end='\r')
        session = sorted(sessions[session_name], key=lambda x: x[time_idx])
        last = 0
        event_num = len(session)
        for index in range(0, event_num):
            curr_event = session[index]
            curr_event_module = curr_event[module_idx]
            curr_event_type = curr_event[event_type_idx]
            if video_events.get(curr_event_module) == None:
                video_events[curr_event_module] = {}
                video_users[curr_event_module] = {}
            if video_events[curr_event_module].get(curr_event_type) == None:
                video_events[curr_event_module][curr_event_type] = [
                    0] * (int(videos_duration[curr_event_module]) + 1)
                video_users[curr_event_module][curr_event_type] = []
                for i in range(0, int(videos_duration[curr_event_module]) + 1):
                    video_users[curr_event_module][curr_event_type].append([])

            video_events[curr_event_module][curr_event_type][int(
                curr_event[current_time_idx])] += 1
            video_users[curr_event_module][curr_event_type][int(
                curr_event[current_time_idx])].append(curr_event[user_idx])

            next_event = session[index + 1] if index < event_num - 1 else None
            if index == event_num - 1 or next_event[time_idx] - curr_event[time_idx] > operation_time_timeout:
                if user_events.get(curr_event[user_idx]) == None:
                    user_events[curr_event[user_idx]] = {}
                user_event = user_events[curr_event[user_idx]]
                if user_event.get(curr_event_module) == None:
                    user_event[curr_event_module] = []

                last_event = session[last]
                user_event[curr_event_module].append({
                    'start': last_event[time_idx],
                    'end': curr_event[time_idx] + operation_time_timeout * 0.2,
                    'length': curr_event[time_idx] - last_event[time_idx] + operation_time_timeout * 0.2,
                    'events': [session[i][event_type_idx] for i in range(last, index + 1)],
                })
                last = index + 1
            elif next_event[module_idx] != curr_event_module:
                if user_events.get(curr_event[user_idx]) == None:
                    user_events[curr_event[user_idx]] = {}
                user_event = user_events[curr_event[user_idx]]
                if user_event.get(curr_event_module) == None:
                    user_event[curr_event_module] = []

                last_event = session[last]
                user_event[curr_event_module].append({
                    'start': last_event[time_idx],
                    'end': next_event[time_idx],
                    'length': next_event[time_idx] - last_event[time_idx],
                    'events': [session[i][event_type_idx] for i in range(last, index + 2)],
                })
                last = index + 1
    print(f'{counter} sessions have been processed.')

    data_dirs = (f'{course}/databaseData/{x}' for x in os.listdir(f'{course}/databaseData/')
                 if os.path.isdir(f'{course}/databaseData/{x}') and 'ignore' not in x)

    db.modules.drop()
    db.modules.create_index([('id', pymongo.HASHED)])
    module_index = {}
    parent_index = []
    all_chapters = []
    for data_dir in data_dirs:
        data_files = (x for x in os.listdir(data_dir)
                      if '.json' in x and 'course_structure' in x)
        for data_file in data_files:
            counter = 0
            print(f'processing {data_dir}/{data_file}')
            with open(f'{data_dir}/{data_file}', 'r') as jsonin:
                courseStructure = json.load(jsonin)
                for k in courseStructure.keys():
                    x = courseStructure[k]
                    category = x['category']
                    if category not in categories:
                        categories.append(category)
                    item = {
                        'children': [l.split('@')[-1] for l in getOffspring(k)],
                        'category': category,
                        'id': k.split('@')[-1],
                        'index': counter,
                    }
                    module_index[item['id']] = counter
                    if category == 'chapter':
                        all_chapters.append(item['id'])
                    for y in x['metadata']:
                        item[y] = x['metadata'][y]
                    if item['category'] == 'problem':
                        tree = ET.ElementTree(file=f'{data_dir}/{course_name}/problem/{k.split("/")[-1].split("@")[-1]}.xml')
                        item['content'] = ET.tostring(tree.getroot()[0]).decode()

                    db.modules.insert_one(item)
                    counter += 1
                for k in courseStructure.keys():
                    x = courseStructure[k]
                    if x.get('parent') == None:
                        parent = -1
                    else:
                        parent = module_index.get(x.get('parent').split('@')[-1])
                    parent_index.append(parent)
               # print(parent_index)
               # print(module_index)
            print(f'{counter} course modules have been processed.')
            module_num = counter

        data_files = (x for x in os.listdir(data_dir) if '.sql' in x)
        for data_file in data_files:
            print(f'processing {data_dir}/{data_file}')
            if 'auth_user-prod' in data_file:
                parseUser(f'{data_dir}/{data_file}')
            elif 'auth_userprofile-prod' in data_file:
                parseUserProfile(f'{data_dir}/{data_file}')
            elif 'certificates_generatedcertificate' in data_file:
                parseCertificate(f'{data_dir}/{data_file}')
            elif 'courseware_studentmodule' in data_file:
                parseCoursewareStudentModule(f'{data_dir}/{data_file}')
            elif 'student_courseenrollment' in data_file:
                parseStudentEnrollment(f'{data_dir}/{data_file}')
            elif 'user_id_map' in data_file:
                parseUserIdMap(f'{data_dir}/{data_file}')

    if enable_activies_insert:
        db.users.drop()
        db.users.create_index([('user_id', pymongo.HASHED)])
        db.users.create_index([('grade', pymongo.ASCENDING)])

        db.problem_activies.drop()
        db.problem_activies.create_index([('user_id', pymongo.HASHED)])
        db.problem_activies.create_index([('id', pymongo.HASHED)])

        db.video_activies.drop()
        db.video_activies.create_index([('user_id', pymongo.HASHED)])
        db.video_activies.create_index([('id', pymongo.HASHED)])

        db.chapter_activies.drop()
        db.chapter_activies.create_index([('user_id', pymongo.HASHED)])
        db.chapter_activies.create_index([('id', pymongo.HASHED)])

    counter = 0
    problem_activies = []
    video_activies = []
    chapter_activies = []
    userset = []
    user_num = len(users.keys())
    video_release_date = {}

    for uid in users:
        counter += 1
        if counter % 100 == 0:
            print(f'{counter} / {user_num} users have been inserted into database...', end='\r')
            if enable_activies_insert:
                if len(userset) > 0:
                    db.users.insert_many(userset)
                if len(chapter_activies) > 0:
                    db.chapter_activies.insert_many(chapter_activies)
                if len(video_activies) > 0:
                    db.video_activies.insert_many(video_activies)
                if len(problem_activies) > 0:
                    db.problem_activies.insert_many(problem_activies)
            problem_activies = []
            video_activies = []
            chapter_activies = []
            userset = []

        user = users[uid]

        if user.get('is_staff', 'NULL') != 'NULL':
            user['is_staff'] = int(user['is_staff'])
        if user.get('is_active', 'NULL') != 'NULL':
            user['is_active'] = int(user['is_active'])
        if user.get('is_superuser', 'NULL') != 'NULL':
            user['is_superuser'] = int(user['is_superuser'])
        if user.get('grade', 'NULL') != 'NULL':
            user['grade'] = float(user.get('grade'))
        if user.get('last_login', 'NULL') != 'NULL':
            user['last_login'] = convertDateString(user.get('last_login'))
        if user.get('date_joined', 'NULL') != 'NULL':
            user['date_joined'] = convertDateString(user.get('date_joined'))
        if user.get('created_date', 'NULL') != 'NULL':
            user['created_date'] = convertDateString(user.get('created_date'))
        if user.get('modified_date', 'NULL') != 'NULL':
            user['modified_date'] = convertDateString(
                user.get('modified_date'))
        if user.get('created', 'NULL') != 'NULL':
            user['created'] = convertDateString(user.get('created'))
        user['events'] = []

        if enable_activies_insert:
            grades = [0] * module_num
            video_watch_times = [0] * module_num
            problems = user_modules['problem'].get(uid)
            if problems == None:
                problems = []
            for problem in problems:
                if problem.get('max_grade', 'NULL') == 'NULL':
                    continue
                if courseStructure.get(problem['module_id']) == None:
                    continue
                grade = float(problem.get('grade', 0))
                max_grade = float(problem.get('max_grade'))
                weight = courseStructure[problem['module_id']].get('metadata', {}).get('weight', 0)
                pid = problem['module_id'].split('@')[-1]
                state = json.loads(problem['state'].replace(r'\\"', "'"))
                activity = {
                    'id': pid,
                    'user_id': uid,
                    'grade': grade,
                    'final': user.get('grade', 0),
                    'max_grade': max_grade,
                    'weight': weight,
                    'created': convertDateString(problem['created']),
                    'modified': convertDateString(problem['modified']),
                }
                if state.get('attempts', 'NULL') != 'NULL':
                    activity['attempts'] = int(state.get('attempts'))
                if state.get('seed', 'NULL') != 'NULL':
                    activity['seed'] = int(state.get('seed'))
                if state.get('done', 'NULL') != 'NULL':
                    activity['done'] = bool(state.get('done'))
                if state.get('student_answers', 'NULL') != 'NULL':
                    activity['student_answers'] = state.get('student_answers')
                if state.get('last_submission_time', 'NULL') != 'NULL':
                    activity['last_submission_time'] = time.mktime(datetime.datetime.strptime(
                        state.get('last_submission_time'), '%Y-%m-%dT%H:%M:%SZ').timetuple()
                    )
                problem_activies.append(activity)
                grade = grade * weight
                max_grade = max_grade * weight
                i = module_index.get(pid, -1)
                while i != -1:
                    grades[i] += grade
                    i = parent_index[i]
                user['grades'] = grades

            videos = user_modules['video'].get(uid)
            if videos == None:
                videos = []

            for video in videos:
                if courseStructure.get(video['module_id']) == None:
                    continue
                video_id = video['module_id'].split('@')[-1]

                state = json.loads(video['state'].replace(r'\\"', "'"))
                user_event = user_events.get(uid)
                events = user_event.get(video_id, []) if user_event != None else []
                video_watch_time = 0
                for x in events:
                    video_watch_time += x['length']
                video['created'] = convertDateString(video['created'])
                video['modified'] = convertDateString(video['modified'])

                if video_release_date.get(video_id) == None or video_release_date.get(video_id) > video['created']:
                    video_release_date[video_id] = video['created']

                for event in events:
                    user['events'].append({
                        'start': event['start'],
                        'end': event['end'],
                        'module': module_index.get(video_id, -1),
                    })

                activity = {
                    'id': video_id,
                    'user_id': uid,
                    'final': user.get('grade', 0),
                    'events': events,
                    'video_watch_time': video_watch_time,
                    'attempts': len(events),
                    'created': video['created'],
                    'modified': video['modified'],
                }
                if state.get('saved_video_position', 'NULL') != 'NULL':
                    activity['saved_video_position'] = state.get(
                        'saved_video_position')

                i = module_index.get(video_id, -1)
                while i != -1:
                    video_watch_times[i] += video_watch_time
                    i = parent_index[i]
                user['video_watch_times'] = video_watch_times

                video_activies.append(activity)
            user['events'] = sorted(user['events'], key = lambda x: x['start'])

            chapters = user_modules['chapter'].get(uid)
            if chapters == None:
                chapters = []
            for chapter in chapters:
                if courseStructure.get(chapter['module_id']) == None:
                    continue
                state = json.loads(chapter['state'].replace(r'\\"', "'"))
                activity = {
                    'id': chapter['module_id'].split('@')[-1],
                    'user_id': uid,
                    'created': convertDateString(chapter['created']),
                    'modified': convertDateString(chapter['modified']),
                }
                if state.get('position', 'NULL') != 'NULL':
                    activity['position'] = state.get('position')
                chapter_activies.append(activity)

        if user.get('password') != None:
            user.pop('password')
        if user.get('show_country') != None:
            user.pop('show_country')
        if user.get('email_tag_filter_strategy') != None:
            user.pop('email_tag_filter_strategy')
        if user.get('display_tag_filter_strategy') != None:
            user.pop('display_tag_filter_strategy')
        if user.get('user_id') != None:
            year = user.get('year_of_birth', 0)
            if year == 'NULL' or year == 0:
                year = '1994'
            user['year_of_birth'] = int(year)
            userset.append(user)

    if enable_activies_insert:
        print(f'{counter} users have been insert into database.')
        if len(userset) > 0:
            db.users.insert_many(userset)
        if len(chapter_activies) > 0:
            db.chapter_activies.insert_many(chapter_activies)
        if len(video_activies) > 0:
            db.video_activies.insert_many(video_activies)
        if len(problem_activies) > 0:
            db.problem_activies.insert_many(problem_activies)
    else:
        print()
    problem_activies = []
    video_activies = []
    chapter_activies = []
    userset = []

    db.videos.drop()
    db.videos.create_index([('id', pymongo.HASHED)])

    counter = 0
    for video_id in video_events:
        video_peaks = {}
        video_user_set = set()
        for event_type in video_events[video_id]:
            if event_type not in peak_detection_types or videos_duration.get(video_id) < min_video_duration:
                continue
            clickstream = video_events[video_id].get(event_type)
            if clickstream != None:# and sum(clickstream) > videos_duration[video_id] * 3:
                smoothed_clickstream = clickstream
                # [0, 0] + np.divide(np.add(np.add(clickstream[2: -5], clickstream[3: -4]), clickstream[4: -3]), 3).tolist()

                video_peaks[event_type] = peak_detection2(smoothed_clickstream)

                for i in range(0, videos_duration[video_id] + 1):
                    for uid in video_users[video_id][event_type][i]:
                        video_user_set.add(uid)

                for peak in video_peaks[event_type]:
                    lo = max(0, peak['start'] - 2)
                    hi = min(videos_duration[video_id], peak['end'] + 2)
                    peak_user_set = set()
                    for i in range(lo, hi + 1):
                        for uid in video_users[video_id][event_type][i]:
                            peak_user_set.add(uid)
                    peak_grades = [users.get(uid, {}).get(
                        'grade', 0) for uid in peak_user_set]
                    if len(peak_grades) == 0 or len(peak_user_set) < min_peak_size:
                        peak['grade_distribution'] = None
                    else:
                        peak['activeness'] = len(peak_user_set)
                        peak['average_grade'] = np.average(peak_grades)
                        peak['grade_distribution'] = value_freq(peak_grades)
                        peak['entropy'] = entropy(peak_grades)
                        peak['users'] = list(peak_user_set)
                video_peaks[event_type] = [peak for peak in video_peaks[event_type] if peak.get(
                    'grade_distribution') != None]
            else:
                video_peaks[event_type] = []
        video_grades = [users.get(uid, {}).get(
            'grade', 0) for uid in video_user_set]
        freq = value_freq(video_grades) if len(video_grades) > 0 else []
        counter += 1
        print(f'{counter} videos have been inserted into database...', end='\r')

        if len(video_user_set) > 0:
            db.videos.insert_one({
                'id': video_id,
                'peaks': video_peaks,
                'release_date': video_release_date.get(video_id),
                'clickstream': video_events[video_id],
                'duration': videos_duration.get(video_id, 0),
                'grade_distribution': freq,
                'average_grade': np.average(video_grades) if len(video_grades) > 0 else 0,
                'activeness': len(video_user_set),
                'entropy': entropy_on_freq(freq) if len(freq) > 0 else 10,
            })
