import os
import csv
import pymongo
import json
import time
import datetime

operation_time_timeout = 300

dbname = 'NarrativeMOOC'
filepath = '/Users/lizhen/data/vismooc'
client = pymongo.MongoClient('localhost', 27017)
users = {}
userModules = {}
courseStructure = {}
categories = []
enable_events_insert = True

def convertDateString(s):
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
                if userModules.get(row[mid]) == None:
                    userModules[row[mid]] = {}
                if userModules[row[mid]].get(row[uid]) == None:
                    userModules[row[mid]][row[uid]] = []
                item = {}
                for i, x in enumerate(row):
                    item[header[i]] = x
                userModules[row[mid]][row[uid]].append(item)
            else:
                print(row, len(row), len(header))
                continue
        print(f'{counter} rows have been loaded.', end='\r')

def parseStudentEnrollment(filename):
    with open(filename, 'r') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        header = None
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
    userModules = {}

    dbname = 'NarrativeMOOC' + course.split('/')[-1]
    db = client[dbname]

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
    log_attributes = ['event_type', 'ip', 'session', 'event_source', 'time', 'page']
    log_event_attributes = ['id', 'currentTime', 'oldTime', 'newTime', 'code', 'type']
    log_context_attributes = ['course_id', 'user_id', 'path']

    has_tsv = False
    for x in (f'{course}/eventData/{x}' for x in os.listdir(f'{course}/eventData/') if x.endswith('.tsv')):
        has_tsv = True
        
    if not has_tsv:
        header = ['event_type', 'user_id', 'module_id', 'ip', 'session', 'event_source', 'time', 'current_time', 'new_time', 'code', 'type', 'path']
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
                    try:
                        x = json.loads(x)
                        logtime = time.mktime(datetime.datetime.strptime(
                            x.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S').timetuple()
                        )
                        event = json.loads(x.get('event'))
                        context = x.get('context')
                        if event.get('old_time') != None:
                            current_time = event.get('old_time')
                        else:
                            current_time = event.get('currentTime')
                        event_id = event.get('id')
                        log = [x.get('event_type'), context.get('user_id'), event_id, x.get('ip'), 
                                x.get('session'), x.get('event_source'), logtime, current_time, 
                                event.get('new_time'), event.get('code'), event.get('type'), context.get('path')]
                        writer.writerow(log)
                        if counter % 10000 == 0:
                            print(f'{counter} records have been processed...', end='\r')
                    except:
                        pass
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
    counter = 0
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
                else:
                    row[time_idx] = float(row[time_idx])
                    row[user_idx] = str(row[user_idx])
                    row[module_idx] = row[module_idx].replace('i4x-', 'i4x://').replace('-', '/')
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
    counter = 0
    for session_name in sessions:
        counter += 1
        if counter % 10000 == 0:
            print(f'{counter} sessions have been processed...', end='\r')
        session = sorted(sessions[session_name], key=lambda x:x[time_idx])
        last = 0
        event_num = len(session)
        for index in range(0, event_num):
            curr_event = session[index]
            next_event = session[index + 1] if index < event_num - 1 else None
            if index == event_num - 1 or next_event[time_idx] - curr_event[time_idx] > operation_time_timeout:
                if user_events.get(curr_event[user_idx]) == None:
                    user_events[curr_event[user_idx]] = {}
                user_event = user_events[curr_event[user_idx]]
                if user_event.get(curr_event[module_idx]) == None:
                    user_event[curr_event[module_idx]] = []

                last_event = session[last]
                user_event[curr_event[module_idx]].append({
                    'start': last_event[time_idx],
                    'end': curr_event[time_idx] + operation_time_timeout * 0.2,
                    'length': curr_event[time_idx] - last_event[time_idx] + operation_time_timeout * 0.2,
                    'events': [session[i][event_type_idx] for i in range(last, index + 1)],
                })
                last = index + 1
            elif next_event[module_idx] != curr_event[module_idx]:
                if user_events.get(curr_event[user_idx]) == None:
                    user_events[curr_event[user_idx]] = {}
                user_event = user_events[curr_event[user_idx]]
                if user_event.get(curr_event[module_idx]) == None:
                    user_event[curr_event[module_idx]] = []

                last_event = session[last]
                user_event[curr_event[module_idx]].append({
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
                        'children': getOffspring(k),
                        'category': category,
                        'id': k,
                    }
                    for y in x['metadata']:
                        item[y] = x['metadata'][y]
                    db.modules.insert_one(item)
                    counter += 1
            print(f'{counter} course modules have been processed.')

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
    for uid in users:
        counter += 1
        if counter % 1000 == 0:
            print(f'{counter} / {user_num} users have been inserted into database...', end='\r')
            db.users.insert_many(userset)
            db.chapter_activies.insert_many(chapter_activies)
            db.video_activies.insert_many(video_activies)
            db.problem_activies.insert_many(problem_activies)
            problem_activies = []
            video_activies = []
            chapter_activies = []
            userset = []

        grades = {}
        user = users[uid]
        problems = userModules['problem'].get(uid)
        if problems == None:
            problems = []
        for problem in problems:
            if problem.get('max_grade', 'NULL') == 'NULL':
                continue
            if courseStructure.get(problem['module_id']) == None:
                continue
            grade = float(problem.get('grade', 0))
            max_grade = float(problem.get('max_grade'))
            weight = courseStructure[problem['module_id']]['metadata']['weight']
            state = json.loads(problem['state'].replace(r'\\"', "'"))
            activity = {
                'id': problem['module_id'],
                'user_id': uid,
                'grade': grade,
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
            grades[problem['module_id']] = grade
            i = courseStructure[problem['module_id']].get('parent')
            while i != None:
                if grades.get(i) == None:
                    grades[i] = grade
                else:
                    grades[i] += grade
                    # grades[i][1] += max_grade
                i = courseStructure[i].get('parent')
            user['grades'] = grades

        videos = userModules['video'].get(uid)
        if videos == None:
            videos = []

        for video in videos:
            if courseStructure.get(video['module_id']) == None:
                continue

            state = json.loads(video['state'].replace(r'\\"', "'"))
            user_event = user_events.get(uid)
            events = user_event.get(video['module_id'], []) if user_event != None else []
            video_watch_time = 0
            for x in events:
                video_watch_time += x['length']

            activity = {
                'id': video['module_id'],
                'user_id': uid,
#                'events': events,
                'video_watch_time': video_watch_time,
                'attempts': len(events),
                'created': convertDateString(video['created']),
                'modified': convertDateString(video['modified']),
            }
            if state.get('saved_video_position', 'NULL') != 'NULL':
                activity['saved_video_position'] = state.get('saved_video_position')

            video_activies.append(activity)

        chapters = userModules['chapter'].get(uid)
        if chapters == None:
            chapters = []
        for chapter in chapters:
            if courseStructure.get(chapter['module_id']) == None:
                continue
            state = json.loads(chapter['state'].replace(r'\\"', "'"))
            activity = {
                'id': chapter['module_id'],
                'user_id': uid,
                'created': convertDateString(chapter['created']),
                'modified': convertDateString(chapter['modified']),
            }
            if state.get('position', 'NULL') != 'NULL':
                activity['position'] = state.get('position')
            chapter_activies.append(activity)
            
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
            user['modified_date'] = convertDateString(user.get('modified_date'))
        if user.get('created', 'NULL') != 'NULL':
            user['created'] = convertDateString(user.get('created'))
        
        del user['password']
        del user['show_country']
        del user['email_tag_filter_strategy']
        del user['display_tag_filter_strategy']
        userset.append(user)

    print(f'{counter} users have been insert into database.')
    db.users.insert_many(userset)
    db.chapter_activies.insert_many(chapter_activies)
    db.video_activies.insert_many(video_activies)
    db.problem_activies.insert_many(problem_activies)
    problem_activies = []
    video_activies = []
    chapter_activies = []
    userset = []
