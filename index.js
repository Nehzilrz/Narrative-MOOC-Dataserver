const Koa = require('koa');
const Router = require('koa-router');
const mongoose = require('mongoose');
const dbUrl = 'mongodb://localhost/NarrativeMOOCintroduceToJava';
// const dbUrl = 'mongodb://localhost/NarrativeMOOCEBA20161T';
const dbUrl2 = 'mongodb://localhost/vismooc';
const cors = require('koa2-cors');
var bodyParser = require('koa-bodyparser');
const stop_words = require('./stop_words').stop_words;
var Redis = require('ioredis');
const Schema = mongoose.Schema;

var redis = new Redis();
const app = new Koa();

const conn = mongoose.createConnection(dbUrl, {
    useMongoClient: true
});
const conn2 = mongoose.createConnection(dbUrl2, {
    useMongoClient: true
});

const user_model_old = conn2.model('users', new Schema({
    birthDate: Number,
    location: String,
    originalId: String,
    educationLevel: String,
    gender: String,
    country: String,
}));

const user_model = conn.model('users', new Schema({
    user_id: String,
    username: String,
    name: String,
    first_name: String,
    last_name: String,
    is_staff: Number,
    is_active: Number,
    is_superuser: Number,
    last_login: Number,
    date_joined: Number,
    created_date: Number,
    modified_date: Number,
    created: Number,
    questions: Number,
    responses: Number,
    status: String,
    country: String,
    country_name: String,
    country_code3: String,
    continent: String,
    city: String,
    year_of_birth: Number,
    gender: String,
    consecutive_days_visit_count: String,
    level_of_education: String,
    certificate_id: String,
    grade: Number,
    course_id: String,
    mode: String,
    grades: [Number],
    video_watch_times: [Number],
    events: [Schema.Types.Mixed],
}));

const event_model = conn.model('events', new Schema({
    event_type: String,
    user_id: String,
    module_id: String,
    ip: String,
    session: String,
    event_source: String,
    time: Number,
}));

const video_model = conn.model('videos', new Schema({
    duration: Number,
    grade_distribution: [Number],
    average_grade: Number,
    activeness: Number,
    entropy: Number,
    id: String,
    release_date: Number,
    peaks: Schema.Types.Mixed,
    clickstream: Schema.Types.Mixed,
}));

const module_model = conn.model('modules', new Schema({
    children: [String],
    category: String,
    id: String,
    display_name: String,
    start: String,
    max_attempts: Number,
    showanswer: String,
    weight: Number,
    index: Number,
    content: String,
    html5_sources: [String],
}));

let course_modules = null;
let id_to_module = {};
let course_chapters = null;

const video_activies_model = conn.model('video_activies', new Schema({
    id: String,
    user_id: String,
    final: Number,
    video_watch_time: Number,
    attempts: Number,
    created: Number,
    modified: Number,
    saved_video_position: String,
    events: [Schema.Types.Mixed],
}));

const forum_thread_model = conn.model('forumthreads', new Schema({
    _type: String,
    id: String,
    user_id: String,
    username: String,
    type: String,
    commentable_id: String,
    anonymous: Boolean,
    anonymous_to_peers : Boolean,
    visible : Boolean,
    floor1 : Boolean,
    comment_count: Number,
    created: Number,
    updated: Number,
    modified: Number,
    title: String,
    body: String,
    _body: String,
    count: Number,
    up_count: Number,
    down_count: Number,
}))

const problem_activies_model = conn.model('problem_activies', new Schema({
    id: String,
    user_id: String,
    grade: Number,
    max_grade: Number,
    final: Number,
    weight: Number,
    video_watch_time: Number,
    attempts: Number,
    created: Number,
    modified: Number,
    last_submission_time: Number,
    student_answers: Schema.Types.Mixed,
}));

function array_is_uniform(vec) {
    const mean = vec.reduce((a, b) => a + b, 0) / vec.length;
    var counter = 0;
    for (const x of vec) {
        if (x * 10 < mean) counter += 1;
    }
    return counter * 2 < vec.length;
}

function array_smooth(vec) {
    const mean = vec.reduce((a, b) => a + b, 0) / vec.length;
    vec = vec.map((d) => Math.min(d, mean * 3));
    return vec.map((d, i) => {
        if (i == 0 || i == vec.length - 1) return d;
        else return (d + vec[i - 1] + vec[i + 1]) / 3;
    });
}

const cachedVideoLogs = {};
let total_user_number = 0;
const id2users = [null];


const user_id_set_cache = {};
const user_set_cache = {};
async function getUserIdSet(condition) {
    const str_cond = JSON.stringify(condition);
    if (user_id_set_cache[str_cond]) {
        return user_id_set_cache[str_cond];
    }
    const ret = (await user_model.find(condition).select('-_id user_id'))
        .map(d => d.user_id);
    return user_id_set_cache[str_cond] = ret;
}

/*
user_id: String,
username: String,
name: String,
first_name: String,
last_name: String,
is_staff: Number,
is_active: Number,
is_superuser: Number,
last_login: Number,
date_joined: Number,
created_date: Number,
modified_date: Number,
created: Number,
status: String,
country: String,
country_name: String,
continent: String,
city: String,
year_of_birth: Number,
gender: String,
consecutive_days_visit_count: String,
level_of_education: String,
certificate_id: String,
grade: Number,
course_id: String,
mode: String,
grades: [Number],
video_watch_times: [Number],
*/
async function getUsers(condition, chapter = null) {
    condition = condition || {};
    const str_cond = JSON.stringify(condition);
    if (user_set_cache[str_cond]) {
        return user_set_cache[str_cond];
    }
    let selector = '-_id user_id country_name year_of_birth continent mode gender grade level_of_education last_login';
    let query = user_model.find(condition).select(selector);
    if (chapter) {
        query = query
            .slice('video_watch_times', [chapter.index, 1])
            .slice('grades', [chapter.index, 1]);
    }
    const ret = await query;
    return user_set_cache[str_cond] = ret;
}

async function getUserSet(condition) {
    if (Array.isArray(condition)) {
        return condition;
    }
    else if (!condition || JSON.stringify(condition) == '{}') {
        return await getUserIdSet({});
    } else {
        const user_ids = await getUserIdSet(condition);
        return user_ids;
    }
}

async function getUserFilter(condition) {
    if (Array.isArray(condition)) {
        const userset = new Set(condition);
        return (x) => userset.has(x);
    }
    else if (!condition || JSON.stringify(condition) == '{}') {
        return () => true;
    } else {
        const user_ids = await getUserIdSet(condition);
        const userset = new Set(user_ids);
        return (x) => userset.has(x);
    }
}

async function getVideoLogs(videoId) {
    if (cachedVideoLogs[videoId] != null) {
        return cachedVideoLogs[videoId];
    }
    const video = await video_model.findOne({ id: videoId }).select("clickstream");

    let sum = 0;
    Object.keys(video.clickstream).forEach((d) => {
        sum += video.clickstream[d].reduce((a, b) => a + b, 0);
    });
    let duration = Math.max(...Object.keys(video.clickstream).map(d => video.clickstream[d].length));
    for (let s = 0, i = 0;; ++i) {
        Object.keys(video.clickstream).forEach((d) => {
            s += video.clickstream[d][i];
        });
        if (s < sum * 0.997) {
            continue;
        } else {
            duration = i;
            break;
        }
    }

    cachedVideoLogs[videoId] = Object.keys(video.clickstream)
        .filter((d) => array_is_uniform(video.clickstream[d].slice(0, duration)))
        .map((d) => ({
            type: d,
            data: array_smooth(video.clickstream[d].slice(0, duration)),
        }));
    return cachedVideoLogs[videoId];
}

const problem_activies_cache = {};
const video_activies_cache = {};

const cachedVideoPeaks = {};
async function getVideoPeaks(videoId) {
    if (cachedVideoPeaks[videoId] != null) {
        return cachedVideoPeaks[videoId];
    }

    const video = await video_model.findOne({ id: videoId }).select("peaks entropy");
    let video_peaks = [];

    for (const action in video.peaks) {
        for (const peak of video.peaks[action]) {
            video_peaks.push({
                action: action,
                index: video_peaks.length,
                entropy_delta: peak.entropy - video.entropy,
                start: peak.start,
                end: peak.end,
                length: peak.length,
                significiant: peak.significiant,
                activeness: peak.activeness,
                average_grade: peak.average_grade * 100,
                grade_distribution: peak.grade_distribution,
                entropy: peak.entropy,
                users: peak.users,
            })
        }
    }

    video_peaks = video_peaks.sort((a, b) => a.significiant - b.significiant).slice(0, 10);
    video_peaks = video_peaks.sort((a, b) => a.start - b.start);
    video_peaks = video_peaks.filter((d, i) => !i || d.start > video_peaks[i - 1].end + 5);
    cachedVideoPeaks[videoId] = video_peaks.sort((a, b) => a.entropy - b.entropy);
    return cachedVideoPeaks[videoId];
}

const ListRouters = new Router();
ListRouters.get("/getVideoList", async ctx => {
    const chapters = course_modules.filter(d => d.category == 'chapter');
    const videos = course_modules.filter(d => d.category == 'video');
    let ret = [];
    for (const v of videos) {
        const info = await video_model.findOne({ id : v.id })
            .select('duration grade_distribution average_grade activeness entropy release_date');
        let currentChapter = null;
        for (const chapter of chapters) {
            if (chapter.children.includes(v.id)) {
                currentChapter = chapter;
                break;
            }
        }
        if (!info || info.activeness == 0) {
            continue;
        }
        ret.push({
            name: v.display_name,
            html5_sources: v.html5_sources[0],
            index: v.index,
            id: v.id,
            sub: v.sub,
            type: 'video',
            duration: info.duration,
            grade_distribution: info.grade_distribution,
            average_grade: info.average_grade * 100,
            activeness: info.activeness.action,
            entropy: info.entropy,
            chapter_name: currentChapter.display_name,
            chapter_start: +(new Date(currentChapter.start)),
            release_date: info.release_date * 1000,
        });
    }
    ctx.body = ret.sort((a, b) => {
        if (a.chapter_start != b.chapter_start) {
            return a.chapter_start - b.chapter_start;
        } else {
            return a.name > b.name ? 1 : -1;
        }
    });
}).get("/getProblemList", async ctx => {
    const problems = course_modules.filter(d => d.category == 'problem');
    const chapters = course_modules.filter(d => d.category == 'chapter');
    let ret = [];
    for (const p of problems) {
        let currentChapter = null;
        for (const chapter of chapters) {
            if (chapter.children.includes(p.id)) {
                currentChapter = chapter;
                break;
            }
        }
        ret.push({
            id: p.id,
            type: 'assignment',
            content: p.content || '',
            name: p.display_name || p.content,
            index: p.index,
            max_attempts: p.max_attempts || 1,
            showanswer: p.showanswer || true,
            submission_wait_seconds: p.submission_wait_seconds,
            chapter_name: currentChapter && currentChapter.display_name,
            chapter_start: (currentChapter && +(new Date(currentChapter.start))) || 1e11,
            weight: p.weight,
        });
    }
    ctx.body = ret.filter((d) => d.name).sort((a, b) => {
        if (a.chapter_start != b.chapter_start) {
            return a.chapter_start - b.chapter_start;
        } else {
            return a.name > b.name ? 1 : -1;
        }
    });
}).get("/getChapterList", async ctx => {
    const chapters = course_modules.filter(d => d.category == 'chapter');
    const videos = course_modules.filter(d => d.category == 'video').map((d) => d.id);
    const problems = course_modules.filter(d => d.category == 'problem').map((d) => d.id);

    ctx.body = chapters.map(c => ({
        name: c.display_name,
        id: c.id,
        start: +(new Date(c.start)),
        type: 'chapter',
        problems: c.children.filter(d => problems.includes(d)),
        index: c.index,
        videos: c.children.filter(d => videos.includes(d)),
    })).sort((a, b) => a.start - b.start);
});

const APIGetRouters = new Router();
APIGetRouters.get("/getVideoLogs", async ctx => {
    const videoId = ctx.query.videoId;
    const clickstream = await getVideoLogs(videoId);
    ctx.body = clickstream;
}).get("/getVideoPeaks", async ctx => {
    const videoId = ctx.query.videoId;
    const peaks = await getVideoPeaks(videoId);
    ctx.body = peaks;
}).get("/countEvent", async ctx => {
    const query = {};
    if (ctx.query.event_type) {
        query['event_type'] = ctx.query.event_type;
    }
    let time_scale = ctx.query.time_scale;
    if (time_scale == 'day') {
        time_scale = 3600 * 24;
    } else if (time_scale == 'hour') {
        time_scale = 3600;
    } else if (!time_scale) {
        time_scale = 3600;
    }
    let time_length = ctx.query.time_length || 168;
    let start_date = ~~(ctx.query.start / 1000);
    start_date = start_date - start_date % time_scale;

    query['time'] = {};
    const ret = [];
    for (let i = 0; i < time_length; ++i) {
        query['time']['$gte'] = start_date + i * time_scale;
        query['time']['$lt'] = start_date + (i + 1) * time_scale;
        const d = await event_model.count(query);
        ret.push({
            date: (i * time_scale + start_date) * 1000,
            val: d,
        });
    }
    ctx.body = ret;
});

const APIPostRouters = new Router();
APIPostRouters.get("/getProblemActivies", async ctx => {
    const activies = await problem_activies_model.find({ id: ctx.query.id });
    ctx.body = activies.map(d => ({
        id: d.id,
        user_id: d.user_id,
        video_watch_time: d.video_watch_time,
        grade: d.grade,
        max_grade: d.max_grade,
        final: d.final * 100,
        weight: d.weight,
        attempts: d.attempts,
        
        created: d.created * 1000,
        modified: d.modified * 1000,
        last_submission_time: d.last_submission_time * 1000,
    }));
}).post("/getProblemsData", async ctx => {
    const problems = ctx.request.body.problems;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const condition = {};
    condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;

    const ret = [];
    for (const pid of problems) {
        const activies = problem_activies_cache[pid] ?
        problem_activies_cache[pid] :
        (problem_activies_cache[pid] = await problem_activies_model.find({ id: pid }));
        const problem = await module_model.findOne({ id: pid });
        if (activies.length == 0) {
            continue;
        }
        let average_grade = 0;
        let average_correctness = 0;
        let average_final = 0;
        let average_created = 0;
        let average_modified = 0;
        let average_duration = 0;
        let average_attempts = 0;
        let average_completeness = 0;
        let user_number = 0;
        for (const x of activies) {
            if (!user_allowed(x.user_id)) continue;
            user_number += 1;
            average_grade += x.grade;
            average_correctness += x.grade / x.max_grade;
            average_final += x.final || 0;
            average_created += x.created * 1000;
            average_modified += x.modified * 1000;
            average_duration += (x.modified - x.created) * 1000;
            average_attempts += x.attempts || 0;
            average_completeness += (x.attempts || 0) >= 1;
        }
        if (user_number == 0) continue;
        average_grade /= user_number;
        average_correctness /= user_number;
        average_final /= user_number;
        average_created /= user_number;
        average_modified /= user_number;
        average_duration /= user_number;
        average_attempts /= user_number;
        average_completeness /= user_number;
        ret.push({
            id: pid,
            correctness: average_correctness,
            grade: average_grade,
            max_grade: activies[0].max_grade,
            final: average_final,
            attempts: average_attempts,
            name: problem.display_name,
            max_attempts: problem.max_attempts,
            weight: problem.weight,
            created: average_created,
            modified: average_modified,
            duration: average_duration,
            activeness: user_number,
            completeness: average_completeness,
        });
    }
    const max_duration = Math.max(...ret.map(d => d.duration));
    const max_activeness =  Math.max(...ret.map(d => d.activeness));
    ret.forEach(d => d.work_time = 
        (d.activeness / max_activeness) * 
        (d.duration / max_duration) * d.attempts * (d.max_grade / d.grade) * 200,
    );
    ctx.body = ret;
}).post("/getVideosData", async ctx => {
    const videos = ctx.request.body.videos;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const condition = {};
    condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;

    const ret = [];
    for (const vid of videos) {
        const activies = video_activies_cache[vid] ?
        video_activies_cache[vid] :
        (video_activies_cache[vid] = await video_activies_model.find({ id: vid }));
        const video = await module_model.findOne({ id: vid });
        const duration = (await video_model.findOne({ id: vid }).select("duration") || {}).duration || 60;
        if (activies.length == 0) {
            continue;
        }
        let average_final = 0;
        let average_created = 0;
        let average_modified = 0;
        let average_attempts = 0;
        let average_video_watch_time = 0;
        let average_completeness = 0;
        let user_number = 0;
        for (const x of activies) {
            if (!user_allowed(x.user_id)) continue;
            user_number += 1;
            average_final += x.final || 0;
            average_created += x.created * 1000;
            average_modified += x.modified * 1000;
            const times = (x.events || [])
                .filter(d => d.start >= start_time && d.start <= end_time)
                .map(d => d.length);
            average_attempts += times.length;
            const tot_time = times.reduce((a, b) => a + b, 0);
            average_video_watch_time += tot_time;
            average_completeness += Math.min(1, tot_time / duration);
            // average_completeness += tot_time > duration * 0.6;
        }
        if (user_number == 0) continue;
        average_final /= user_number;
        average_created /= user_number;
        average_modified /= user_number;
        average_attempts /= user_number;
        average_video_watch_time /= user_number;
        average_completeness /= user_number;
        ret.push({
            id: vid,
            final: average_final,
            video_watch_time: average_video_watch_time,
            attempts: average_attempts,
            name: video.display_name,
            created: average_created,
            modified: average_modified,
            activeness: user_number,
            completeness: average_completeness,
        });
    }
    ctx.body = ret;
}).post("/getVideoActiviesDistribution", async ctx => {
    let ret = [];
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    if (chapter) {
        const condition = {};
        condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
        const start_time = (new Date(chapter.start)) / 1000;
        const end_time = start_time + 86400 * 7;
        let users = await user_model.find(condition)
            .slice('video_watch_times', [chapter.index, 1])
            .slice('grades', [chapter.index, 1])
            .select("-_id user_id events");
        if (ctx.query.student == "good") {
            users = users.sort((a, b) => b.grades[0] - a.grades[0]);
            users = users.slice(0, ~~(users.length * 0.2));
        } else if (ctx.query.student == "bad") {
            users = users.sort((a, b) => a.grades[0] - b.grades[0]);
            users = users.slice(0, ~~(users.length * 0.2));
        }
        const video_access_records = {};
        for (const user of users) {
            if (!user_allowed(user.user_id)) continue;
            const events = user.events.filter((d => d.start >= start_time && d.start <= end_time));
            const access_records = {};
            for (const event of events) {
                if (!access_records[event.module]) {
                    access_records[event.module] = 0;
                }
                access_records[event.module] += event.end - event.start;
            }
            for (const x of Object.keys(access_records)) {
                if (!video_access_records[x]) {
                    video_access_records[x] = [];
                }
                video_access_records[x].push(access_records[x]);
            }
        }
        for (const video of Object.keys(video_access_records)) {
            const records = video_access_records[video].sort((a, b) => a - b);
            const partition_num = 10;
            if (records.length < partition_num) {
                continue;
            }
            const distribution = [];
            for (let i = 0; i < partition_num; ++i) {
                const lo = ~~(records.length * i / partition_num);
                const hi = ~~(records.length * (i + 1) / partition_num);
                let sum = 0;
                for (let j = lo; j < hi; ++j) {
                    sum += records[j];
                }
                distribution.push({
                    min: records[lo], max: records[hi - 1], avg: sum / (hi - lo),  
                });
            }
            ret.push({
                video_id: course_modules[video].id,
                distribution: distribution,
                activeness: records.length,
            });
        }
    }
    ctx.body = ret.sort((a, b) => b.activeness - a.activeness);
}).post("/getProblemGradesDistribution", async ctx => {
    let ret = [];
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    if (chapter) {
        const condition = {};
        condition[`grades.${chapter.index}`] = {$gt: 0};
        const start_time = (new Date(chapter.start)) / 1000;
        const end_time = start_time + 86400 * 7;
        let users = await user_model.find(condition)
            .slice('video_watch_times', [chapter.index, 1])
            .slice('grades', [chapter.index, 1])
            .select("-_id user_id grades");
        users = users.sort((a, b) => b.grades[0] - a.grades[0]);
        if (ctx.query.student == "good") {
            users = users.sort((a, b) => b.grades[0] - a.grades[0]);
            users = users.slice(0, ~~(users.length * 0.2));
        } else if (ctx.query.student == "bad") {
            users = users.sort((a, b) => a.grades[0] - b.grades[0]);
            users = users.slice(0, ~~(users.length * 0.2));
        }
        const video_access_records = {};
        for (const user of users) {
            if (!user_allowed(user.user_id)) continue;
            const events = user.events.filter((d => d.start >= start_time && d.start <= end_time));
            const access_records = {};
            for (const event of events) {
                if (!access_records[event.module]) {
                    access_records[event.module] = 0;
                }
                access_records[event.module] += event.end - event.start;
            }
            for (const x of Object.keys(access_records)) {
                if (!video_access_records[x]) {
                    video_access_records[x] = [];
                }
                video_access_records[x].push(access_records[x]);
            }
        }
        for (const video of Object.keys(video_access_records)) {
            const records = video_access_records[video].sort((a, b) => a - b);
            const partition_num = 10;
            if (records.length < partition_num) {
                continue;
            }
            const distribution = [];
            for (let i = 0; i < partition_num; ++i) {
                const lo = ~~(records.length * i / partition_num);
                const hi = ~~(records.length * (i + 1) / partition_num);
                let sum = 0;
                for (let j = lo; j < hi; ++j) {
                    sum += records[j];
                }
                distribution.push({
                    min: records[lo], max: records[hi - 1], avg: sum / (hi - lo),  
                });
            }
            ret.push({
                video_id: course_modules[video].id,
                distribution: distribution,
                activeness: records.length,
            });
        }
    }
    ctx.body = ret.sort((a, b) => b.activeness - a.activeness);
}).post("/getChapterVideosInfo", async ctx => {
    let ret = [];
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const allvideos = course_modules.filter(d => d.category == 'video').map((d) => d.id);
    const videos = chapter.children.filter(d => allvideos.includes(d));
    if (chapter) {
        const condition = {};
        condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
        let start_time = (new Date(chapter.start)) / 1000;
        let end_time = start_time + 86400 * 7;
        let users = await user_model.find(condition)
            .slice('video_watch_times', [chapter.index, 1])
            .slice('grades', [chapter.index, 1])
            .select("-_id user_id events");
        users = users.sort((a, b) => a.grades[0] - b.grades[0]);
        const records = {};
        for (let i = 0; i < users.length; ++i) {
            const user = users[i];
            if (!user_allowed(user.user_id)) continue;
            const events = user.events.filter((d => d.start >= start_time && d.start <= end_time));
            for (const event of events) {
                if (!records[event.module]) {
                    records[event.module] = [0, 0, 0, 0, 0];
                }
                records[event.module][~~(i * 5 / users.length)] += event.end - event.start;
            }
        }
        for (const index of Object.keys(records)) if (videos.includes(course_modules[index].id)) {
            ret.push({
                id: course_modules[index].id,
                length: [{
                    name: 'the worst 20%',
                    value: records[index][0] / (users.length / 5),
                }, {
                    name: '20% - 60%',
                    value: (records[index][1] + records[index][2]) / (users.length / 5 * 2),
                }, {
                    name: '60% - 80%',
                    value: records[index][3] / (users.length / 5),
                }, {
                    name: 'the top 20%',
                    value: records[index][4] / (users.length / 5),
                }],
            });
        }
    }
    ctx.body = ret.sort((a, b) => a.id > b.id ? 1 : -1);
}).post("/getChapterProblemsInfo", async ctx => {
    let ret = [];
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const problems = course_modules.filter(d => 
        d.category == 'problem' && chapter.children.includes(d.id)
    ).map(d => d.index);
    if (chapter) {
        const condition = {};
        condition[`grades.${chapter.index}`] = {$gt: 0};
        let start_time = (new Date(chapter.start)) / 1000;
        let end_time = start_time + 86400 * 7;
        let users = await user_model.find(condition)
            .select("-_id user_id grades");
        users = users.sort((a, b) => a.grades[chapter.index] - b.grades[chapter.index]);
        const records = {};
        for (let i = 0; i < users.length; ++i) {
            const user = users[i];
            if (!user_allowed(user.user_id)) continue;
            for (const index of problems) {
                if (!records[index]) {
                    records[index] = [0, 0, 0, 0, 0];
                }
                records[index][~~(i * 5 / users.length)] += user.grades[index];
            }
        }
        for (const index of Object.keys(records)) if (chapter.children.includes(course_modules[index].id)) {
            ret.push({
                id: course_modules[index].id,
                length: [{
                    name: 'the worst 20%',
                    value: records[index][0] / (users.length / 5),
                }, {
                    name: '20% - 60%',
                    value: (records[index][1] + records[index][2]) / (users.length / 5 * 2),
                }, {
                    name: '60% - 80%',
                    value: records[index][3] / (users.length / 5),
                }, {
                    name: 'the top 20%',
                    value: records[index][4] / (users.length / 5),
                }],
            });
        }
    }
    ctx.body = ret.sort((a, b) => a.id > b.id ? 1 : -1);
}).post("/getUserBasicInfo", async ctx => {
    let users = [];
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    if (!chapter) return;
    const chapter_start = (+new Date(chapter.start)) / 1000;
    const chapter_end = chapter_start + 86400 * 7;
    let condition = ctx.request.body.condition;
    if (Array.isArray(condition)) {
        for (const uid of condition) {
            const user = await user_model.findOne({ user_id: uid }).select(
                "-_id country_name year_of_birth continent mode gender grade level_of_education last_login"
            );
            if (!user) {
                continue;
            }
            users.push(user);
        }
    } else {
        condition = condition || {};
        condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
        users = await getUsers(condition, chapter);
    }

    // country_name year_of_birth continent mode gender
    // grade level_of_education last_login
    const s_dimensions = ['country_name', 'continent', 'mode', 'gender', 'level_of_education'];
    const i_dimensions = ['grade', 'drop', 'age'];
    const count = {};
    s_dimensions.forEach(d => count[d] = {});
    i_dimensions.forEach(d => count[d] = {});
    count['_grade'] = {};
    count['_age'] = {};

    for (const user of users) {
        let val;
        for (const d of s_dimensions) {
            val = user[d] || '';
            if (!count[d][val]) count[d][val] = 0;
            count[d][val] += 1;
        }
        val = ~~(user.grade * 100);
        if (!count['_grade'][val]) count['_grade'][val] = 0;
        count['_grade'][val] += 1;
        val = ~~(user.grade * 9.9)
        if (!count['grade'][val]) count['grade'][val] = 0;
        count['grade'][val] += 1;

        val = 2014 - (+user.year_of_birth);
        if (user.year_of_birth == '') val = 0;
        if (!count['_age'][val]) count['_age'][val] = 0;
        count['_age'][val] += 1;
        val = Math.ceil(val / 10);
        if (!count['age'][val]) count['age'][val] = 0;
        count['age'][val] += 1;

        val = user.last_login > chapter_end ? 'continue': 'drop';
        if (!count['drop'][val]) count['drop'][val] = 0;
        count['drop'][val] += 1;
    }

    ctx.body = Object.keys(count).map(key => ({
        key: key,
        val: Object.keys(count[key])
        .sort((a, b) => a < b ? -1 : 1)
        .map(name => 
            ({ name, val: count[key][name] })
        ),
        n: Object.values(count[key]).reduce((a, b) => a + b, 0),
    }));
}).post("/getUserNumber", async ctx => {
    let users = [];
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    if (!chapter) return;
    const chapter_start = (+new Date(chapter.start)) / 1000;
    const chapter_end = chapter_start + 86400 * 7;
    let condition = ctx.request.body.condition;
    if (Array.isArray(condition)) {
        for (const uid of condition) {
            const user = await user_model.findOne({ user_id: uid }).select(
                "-_id country_name year_of_birth continent mode gender grade level_of_education last_login"
            );
            if (!user) {
                continue;
            }
            users.push(user);
        }
    } else {
        condition = condition || {};
        condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
        users = await getUsers(condition, chapter);
    }
    ctx.body = users.length;
}).post("/getForumThreadMostReplied", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    let threads = await forum_thread_model.find({ created: { $gt: start_time, $lt: end_time }});
    threads = threads.sort((a, b) => b.comment_count - a.comment_count);
    ctx.body = threads;
}).post("/getForumUserMostActive", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    let threads = await forum_thread_model.find({ created: { $gt: start_time, $lt: end_time }});
    const count = {};
    for (const thread of threads) {
        count[thread.user_id] = count[thread.user_id] || 0;
        count[thread.user_id] += 1;
    }
    const users = Object.keys(count).map(d => ({ id: d, val: count[d] }))
        .sort((a, b) => b.val - a.val);
    ctx.body = users;
}).post("/getForumThreadProblemRelated", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    const threads = (await forum_thread_model.find({ created: { $gt: start_time, $lt: end_time }}))
        .map(d => ({
            thread: d,
            text: (d.title + d._body + d.body).toLowerCase().replace(' ', ''),
            comment_count: d.comment_count == 0 ? 1 : d.comment_count,
        }));
    const ret = ctx.request.body.problems
        .map((id) => id_to_module[id])
        .map((p) => {
            const keyword = p.display_name.toLowerCase().replace(' ', '');
            const targets = threads.filter(d => d.text.includes(keyword));
            return {
                name: p.display_name,
                id: p.id,
                val: targets.length,
                posts: targets.map(d => ({
                    title: d.thread.title,
                    username: d.thread.username,
                    body: d.thread.body 
                })),
            }
        })
        .sort((a, b) => b.val - a.val);
    ctx.body = ret;
}).post("/getForumThreadVideoRelated", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    const threads = (await forum_thread_model.find({ created: { $gt: start_time, $lt: end_time }}))
        .map(d => ({
            thread: d,
            text: (d.title + d._body + d.body).toLowerCase().replace(' ', ''),
            comment_count: d.comment_count == 0 ? 1 : d.comment_count,
        }));
    const ret = ctx.request.body.videos
        .map((id) => id_to_module[id])
        .map((p) => {
            const keyword = p.display_name.toLowerCase().replace(' ', '');
            const targets = threads.filter(d => d.text.includes(keyword));
            return {
                name: p.display_name,
                id: p.id,
                val: targets.length,
                posts: targets.map(d => ({
                    title: d.thread.title,
                    username: d.thread.username,
                    body: d.thread.body
                })),
            }
        })
        .sort((a, b) => b.val - a.val);
    ctx.body = ret;
}).post("/getMostDiscussedThreads", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    const threads = (await forum_thread_model.find({ 
        created: { $gt: start_time, $lt: end_time }
    }));
    const count = {};
    for (const thread of threads) {
        if (!count[thread.id]) {
            count[thread.id] = [];
        }
        count[thread.id].push(thread);
    }
    
    ctx.body = threads.filter(d => d.comment_count).map(d => ({
        body: d.body,
        title: d.title,
        username: d.username,
        comment_count: count[d.id].length,
        comments: count[d.id].map(e => ({
            title: e.title,
            username: e.username,
            body: e.body,
        }))
    })).sort((a, b) => b.comment_count - a.comment_count);
}).post("/getMostDiscussedKeywords", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    var reg= /[^a-z]/g;
    const texts = [].concat(...(await forum_thread_model.find({ 
        created: { $gt: start_time, $lt: end_time },
    })).map(d => ((d._body == '' ? d.title: '') + d.body)
        .toLowerCase()
        .replace(reg, ' ')
        .split(' ')
        .filter(t => t != '')
    ));
    const count = {};
    for (const word of texts) if (!stop_words.has(word) && word.length > 1) {
        count[word] = count[word] || 0;
        count[word] += 1;
    }
    const words = Object.keys(count).map(d => ({ word: d, val: count[d] }))
        .sort((a, b) => b.val - a.val)
        .slice(0, 50);

    ctx.body = words;
}).post("/getMostUpvotedThreads", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;
    const threads = (await forum_thread_model.find({ 
        created: { $gt: start_time, $lt: end_time },
        up_count: { $gt: 0 },
    }));
    ctx.body = threads.sort((a, b) => b.up_count - a.up_count)
        .slice(0, 10)
        .map(d => ({
            title: d.title,
            username: d.username,
            body: d.body,
            up_count: d.up_count,
        }));
}).post("/getTopQuestioners", async ctx => {
    let chapter_id = ctx.request.body.chapter;
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const start_time = (new Date(chapter.start)) / 1000;
    const end_time = start_time + 86400 * 7;

    const threads = (await forum_thread_model.find({ 
        created: { $gt: start_time, $lt: end_time }
    }));
    const questioner_count = {};
    const responder_count = {};

    for (const thread of threads) {
        if (thread.floor1) {
            if (!questioner_count[thread.user_id]) {
                questioner_count[thread.user_id] = [];
            }
            questioner_count[thread.user_id].push(thread);
        } else {
            if (!responder_count[thread.user_id]) {
                responder_count[thread.user_id] = [];
            }
            responder_count[thread.user_id].push(thread);
        }
    }

    const questioners = Object.keys(questioner_count)
        .map(d => ({
            val: questioner_count[d].length,
            user_id: questioner_count[d][0].user_id, 
            username: questioner_count[d][0].username,
            posts: questioner_count[d].map(e => ({
                title: e.title,
                username: e.username,
                body: e.body
            })),
        }))
        .sort((a, b) => b.val - a.val);

    const responders = Object.keys(responder_count)
        .map(d => ({
            val: responder_count[d].length,
            user_id: responder_count[d][0].user_id, 
            username: responder_count[d][0].username,
            posts: responder_count[d].map(e => ({
                title: e.title,
                username: e.username,
                body: e.body
            })),
        }))
        .sort((a, b) => b.val - a.val);

    let max_value = Math.max(...questioners.map(d => d.val));
    let scales = Math.min(10, max_value);
    let questioners_count = [];
    for (let i = 0; i < scales; ++i) {
        const lo = ~~(i / scales * max_value) + 1;
        const hi = ~~((i + 1) / scales * max_value);
        questioners_count[i] = {
            name: lo == hi ? `${lo}`: `${lo} - ${hi}`,
            val: 0,
            users: [],
        };
    }
    for (const x of questioners) {
        const i = ~~((x.val - 1) * scales / max_value);
        questioners_count[i].val++;
        questioners_count[i].users.push(x.user_id);
    }


    max_value = Math.max(...responders.map(d => d.val));
    scales = Math.min(10, max_value);
    let responders_count = [];
    for (let i = 0; i < scales; ++i) {
        const lo = ~~(i / scales * max_value) + 1;
        const hi = ~~((i + 1) / scales * max_value);
        responders_count[i] = {
            name: lo == hi ? `${lo}`: `${lo} - ${hi}`,
            val: 0,
            users: [],
        };
    }
    for (const x of responders) {
        const i = ~~((x.val - 1) * scales / max_value);
        responders_count[i].val++;
        responders_count[i].users.push(x.user_id);
    }
/*
    for (const d of questioners) {
        await user_model.update({ user_id: d.user_id },
            { $set: { questions: d.val }
        });
    }

    for (const d of responders) {
        await user_model.update({ user_id: d.user_id },
            { $set: { responses: d.val }
        });
    }
*/
    ctx.body = {
        questioners: questioners.slice(0, 3),
        responders:  responders.slice(0, 3),
        questioners_count,
        responders_count,
    };
}).post("/getChapterOperationSequence", async ctx => {
    let ret = [];
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    const videos = course_modules.filter(d => 
        d.category == 'video' && chapter.children.includes(d.id)
    ).map(d => ({ id: d.id, index: d.index }));
    const condition = {};
    condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
    condition[`grades.${chapter.index}`] = {$gt: 0};
    let start_time = (new Date(chapter.start)) / 1000;
    let end_time = start_time + 86400 * 7;
    let users = await user_model.find(condition)
        .select("-_id user_id events");
    users = users.filter(d => user_allowed(d.user_id));
    const records = {};

    const user_map = {};
    for (const user of users) {
        user_map[user.user_id] = user;
    }

    const problems = course_modules.filter(d => 
        d.category == 'problem' && chapter.children.includes(d.id)
    ).map(d => ({ id: d.id, index: d.index }));

    for (const p of problems) {
        const activies = problem_activies_cache[p.id] ?
        problem_activies_cache[p.id] :
        (problem_activies_cache[p.id] = await problem_activies_model.find({ id: p.id }));
        for (const x of activies) {
            if (user_map[x.user_id]) {
                user_map[x.user_id].events.push({
                    start: x.created,
                    end: x.last_submission_time,
                    module: p.index,
                });
            }
        }
    }
    const module_indexs = videos.concat(problems).map(d => d.index);
    const module_index_set = new Set(module_indexs);
    const module_index_of = {};
    module_indexs.forEach((d, i) => module_index_of[d] = i);
    const module_ids = videos.concat(problems).map(d => d.id);
    const n = module_indexs.length;
    const count = [];
    for (let i = 0; i < n; ++i) {
        count[i] = [];
        for (let j = 0; j < n; ++j) count[i][j] = 0;
    }

    for (let i = 0; i < users.length; ++i) {
        const user = users[i];
        const events = user.events.filter(d =>
            d.start >= start_time &&
            d.start <= end_time &&
            module_index_set.has(d.module)
        ).sort((a, b) => a.end - b.end);
        for (let j = 1; j < events.length; ++j) {
            count[module_index_of[events[j - 1].module]][module_index_of[events[j].module]] += 1;
        }
    }

    let weight = [];
    for (let i = 0; i < n; ++i) {
        let send = 0, recv = 0;
        for (let j = 0; j < n; ++j) {
            send += count[i][j];
            recv += count[j][i];
        }
        weight[i] = send - recv;
    }

    const order = weight.map((d, i) => ({ index: i, val: d }))
        .sort((a, b) => b.val - a.val)
        .map(d => d.index);
    
    const adj = [];
    for (let i = 0; i < n; ++i) {
        adj[i] = [];
        for (let j = 0; j < n; ++j) {
            adj[i][j] = count[order[i]][order[j]];
        }
    }

    ctx.body = {
        modules: order.map(d => ({
            id: course_modules[module_indexs[d]].id,
            name: course_modules[module_indexs[d]].display_name,
            type: course_modules[module_indexs[d]].category,
        })),
        adj: adj,
    }
}).post("/getAssignmentOperationSequence", async ctx => {
    let ret = [];
    const user_allowed = await getUserFilter(ctx.request.body.condition);
    let problem_id = ctx.request.body.assignment;
    console.log(problem_id);
    const chapter = course_chapters.find((d) => d.children.includes(problem_id));
    const videos = course_modules.filter(d => 
        d.category == 'video' && chapter.children.includes(d.id)
    ).map(d => ({ id: d.id, index: d.index }));
    const condition = {};
    condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
    condition[`grades.${chapter.index}`] = {$gt: 0};
    let start_time = (new Date(chapter.start)) / 1000;
    let end_time = start_time + 86400 * 7;
    let users = await user_model.find(condition)
        .select("-_id user_id events");
    users = users.filter(d => user_allowed(d.user_id));
    const records = {};

    const user_map = {};
    for (const user of users) {
        user_map[user.user_id] = user;
    }

    const problems = course_modules.filter(d => 
        d.id == problem_id
    ).map(d => ({ id: d.id, index: d.index }));

    const module_indexs = [].concat(
        [problems[0].index], 
        videos.map(d => d.index),
        [problems[0].index]);
    const module_index_set = new Set(module_indexs);
    const module_index_of = {};
    module_indexs.forEach((d, i) => module_index_of[d] = i);
    const n = module_indexs.length;

    const average_time = [];
    const count = [];
    for (let i = 0; i < n; ++i) {
        count[i] = [];
        average_time[i] = [0, 0];
        for (let j = 0; j < n; ++j) count[i][j] = 0;
    }

    for (const p of problems) {
        const activies = problem_activies_cache[p.id] ?
        problem_activies_cache[p.id] :
        (problem_activies_cache[p.id] = await problem_activies_model.find({ id: p.id }));
        for (const x of activies) {
            if (user_map[x.user_id]) {
                const start = x.created;
                const end = x.last_submission_time;
                if (start > end_time) continue;
                const events = user_map[x.user_id].events
                    .filter(d =>
                        d.start >= start &&
                        d.start <= end &&
                        module_index_set.has(d.module)
                    ).map(d => ({
                        time: (d.start + d.end) / 2,
                        index: module_index_of[d.module],
                    })).sort((a, b) => a.time - b.time);
                if (events.length == 0) {
                    continue;
                }
                
                average_time[0][0] += start;
                average_time[n - 1][0] += end;
                average_time[0][1] += 1;
                average_time[n - 1][1] += 1;
                events.forEach(d => {
                    if (d.time == 0) return;
                    average_time[d.index][0] += d.time;
                    average_time[d.index][1] += 1;
                });
                count[0][events[0].index] += 1;
                count[events[events.length - 1].index][n - 1] += 1;
                for (let j = 1; j < events.length; ++j) {
                    count[events[j - 1].index][events[j].index] += 1;
                }
            }
        }
    }

    let weight = [];
    for (let i = 0; i < n; ++i) {
        let send = 0, recv = 0;
        for (let j = 0; j < n; ++j) {
            send += count[i][j];
            recv += count[j][i];
        }
        weight[i] = average_time[i][0] / average_time[i][1];
    }

    const order = weight.map((d, i) => ({ index: i, val: d }))
        .sort((a, b) => a.val - b.val)
        .map(d => d.index);
    
    const adj = [];
    for (let i = 0; i < n; ++i) {
        adj[i] = [];
        for (let j = 0; j < n; ++j) {
            adj[i][j] = count[order[i]][order[j]];
        }
    }

    ret = order.map((d, i) => ({
        id: course_modules[module_indexs[d]].id,
        name: course_modules[module_indexs[d]].display_name,
        type: course_modules[module_indexs[d]].category,
        time: average_time[order[i]][0] / average_time[order[i]][1],
        val: average_time[order[i]][1],
    }));

    while (ret.length && ret[ret.length - 1].type == 'video') {
        ret.pop();
        adj.pop();
        for (let i = 0; i < adj.length; ++i) {
            adj[i].pop();
        }
    }

    ctx.body = {
        modules: ret,
        adj: adj,
    }
}).post("/getUserDifficulties", async ctx => {
    let users = [];
    let chapter_id = ctx.request.body.chapter;
    const chapter = course_chapters.find((d) => d.id == chapter_id);
    if (!chapter) return;
    const chapter_start = (+new Date(chapter.start)) / 1000;
    const chapter_end = chapter_start + 86400 * 7;
    let condition = ctx.request.body.condition;
    if (Array.isArray(condition)) {
        for (const uid of condition) {
            const user = await user_model.findOne({ user_id: uid }).select(
                "-_id country_name year_of_birth continent mode gender grade level_of_education last_login"
            );
            if (!user) {
                continue;
            }
            users.push(user);
        }
    } else {
        condition = condition || {};
        condition[`video_watch_times.${chapter.index}`] = {$gt: 0};
        users = await getUsers(condition, chapter);
    }

    ctx.body = users.map(user => ({
        time: user.video_watch_times[0],
        grade: user.grades[0],
        final: user.grade,
    }));
});

async function init() {
    course_modules = (await module_model.find()).sort((a, b) => a.index - b.index);
    course_modules.forEach((d) => id_to_module[d.id] = d);
    course_chapters = course_modules.filter(d => d.category == 'chapter');
    total_user_number = await user_model.count();
}

init();
app.use(cors());
app.use(bodyParser());
/*
app.use(async (ctx, next) => {
    let args = ctx.url;
    if (ctx.method === 'POST') {
        args += JSON.stringify(ctx.request.body);
    } else if (ctx.method == "GET") {
        args += JSON.stringify(ctx.query);
    }
    const ret = await redis.get(args);
    if (ret) {
        console.log(`Find ${args} in cache.`);
        ctx.body = JSON.parse(ret);
        return;
    } else {
        await next();
        if (ctx.body) {
            console.log(`Save ${args} into cache.`);
            redis.set(args, JSON.stringify(ctx.body));
        } else {
            console.log(`Cannot parse the url.`);
        }
    }
});*/

app.use(ListRouters.routes());
app.use(APIGetRouters.routes());
app.use(APIPostRouters.routes());

app.listen(3000);
