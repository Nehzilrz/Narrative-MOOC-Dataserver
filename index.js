const Koa = require('koa');
const Router = require('koa-router')
const mongoose = require('mongoose');
const dbUrl = 'mongodb://localhost/NarrativeMOOCintroduceToJava';
const dbUrl2 = 'mongodb://localhost/vismooc';
const cors = require('koa2-cors');
var bodyParser = require('koa-bodyparser');
const Schema = mongoose.Schema;

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
    status: String,
    country: String,
    year_of_birth: String,
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
    html5_sources: [String],
}));

let course_modules = null;
let course_chapters = null;

const video_activies_model = conn.model('video_activies', new Schema({
    id: String,
    user_id: String,
    final: Number,
    video_watch_time: Number,
    attempts: Number,
    created: Number,
    modified: Number,
    saved_video_position: String
}));

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

//     video_peaks = video_peaks.sort((a, b) => a.significiant - b.significiant).slice(0, 15);
    video_peaks = video_peaks.sort((a, b) => a.start - b.start);
    video_peaks = video_peaks.filter((d, i) => !i || d.start > video_peaks[i - 1].end + 5);
    cachedVideoPeaks[videoId] = video_peaks.sort((a, b) => a.entropy - b.entropy);
    return cachedVideoPeaks[videoId];
}

const Routers = new Router();
Routers.get("/getCourseList", async ctx => {
    ctx.body = [];
}).get("/getVideoList", async ctx => {
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
            name: p.display_name,
            index: p.index,
            max_attempts: p.max_attempts,
            showanswer: p.showanswer,
            submission_wait_seconds: p.submission_wait_seconds,
            chapter_name: currentChapter.display_name,
            chapter_start: +(new Date(currentChapter.start)) || 1e11,
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
        problems: c.children.filter(d => problems.includes(d)),
        index: c.index,
        videos: c.children.filter(d => videos.includes(d)),
    })).sort((a, b) => a.start - b.start);
}).get("/getVideoLogs", async ctx => {
    const videoId = ctx.query.videoId;
    const clickstream = await getVideoLogs(videoId);
    ctx.body = clickstream;
}).get("/getVideoPeaks", async ctx => {
    const videoId = ctx.query.videoId;
    const peaks = await getVideoPeaks(videoId);
    ctx.body = peaks;
}).get("/getProblemActivies", async ctx => {
    const activies = await problem_activies_model.find({ id: ctx.query.id });
    ctx.body = activies.map(d => ({
        id: d.id,
        user_id: d.user_id,
        grade: d.grade,
        max_grade: d.max_grade,
        final: d.final * 100,
        weight: d.weight,
        video_watch_time: d.video_watch_time,
        attempts: d.attempts,
        created: d.created * 1000,
        modified: d.modified * 1000,
        last_submission_time: d.last_submission_time * 1000,
    }));
}).post("/getProblemsData", async ctx => {
    const problems = ctx.request.body.problems;
    const ret = [];
    for (const pid of problems) {
        const activies = await problem_activies_model.find({ id: pid });
        const problem = await module_model.findOne({ id: pid });
        if (activies.length == 0) {
            continue;
        }
        let average_grade = 0;
        let average_final = 0;
        let average_created = 0;
        let average_modified = 0;
        let average_duration = 0;
        let average_attempts = 0;
        let average_video_watch_time = 0;
        for (const x of activies) {
            average_grade += x.grade;
            average_final += x.final || 0;
            average_created += x.created * 1000;
            average_modified += x.modified * 1000;
            average_duration += (x.modified - x.created) * 1000;
            average_attempts += x.attempts || 0;
            average_video_watch_time += x.video_watch_time || 0;
        }
        average_grade /= activies.length;
        average_final /= activies.length;
        average_created /= activies.length;
        average_modified /= activies.length;
        average_duration /= activies.length;
        average_attempts /= activies.length;
        average_video_watch_time /= activies.length;
        ret.push({
            id: pid,
            grade: average_grade,
            max_grade: activies[0].max_grade,
            final: average_final,
            video_watch_time: average_video_watch_time,
            attempts: average_attempts,
            name: problem.display_name,
            max_attempts: problem.max_attempts,
            weight: problem.weight,
            created: average_created,
            modified: average_modified,
            duration: average_duration,
            activeness: activies.length,
        });
    }
    ctx.body = ret;
}).post("/getVideosData", async ctx => {
    const videos = ctx.request.body.videos;
    const ret = [];
    for (const vid of videos) {
        const activies = await video_activies_model.find({ id: vid });
        const video = await module_model.findOne({ id: vid });
        if (activies.length == 0) {
            continue;
        }
        let average_final = 0;
        let average_created = 0;
        let average_modified = 0;
        let average_attempts = 0;
        let average_video_watch_time = 0;
        for (const x of activies) {
            average_final += x.final || 0;
            average_created += x.created * 1000;
            average_modified += x.modified * 1000;
            average_attempts += x.attempts || 0;
            average_video_watch_time += x.video_watch_time || 0;
        }
        average_final /= activies.length;
        average_created /= activies.length;
        average_modified /= activies.length;
        average_attempts /= activies.length;
        average_video_watch_time /= activies.length;
        ret.push({
            id: vid,
            final: average_final,
            video_watch_time: average_video_watch_time,
            attempts: average_attempts,
            name: video.display_name,
            created: average_created,
            modified: average_modified,
            activeness: activies.length,
        });
    }
    ctx.body = ret;
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
}).get("/getVideoActiviesDistribution", async ctx => {
    let ret = [];
    if (ctx.query.chapter) {
        let chapter_id = ctx.query.chapter;
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
    }
    ctx.body = ret.sort((a, b) => b.activeness - a.activeness);
}).get("/getProblemGradesDistribution", async ctx => {
    let ret = [];
    if (ctx.query.chapter) {
        let chapter_id = ctx.query.chapter;
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
    }
    ctx.body = ret.sort((a, b) => b.activeness - a.activeness);
}).get("/getChapterVideosInfo", async ctx => {
    let ret = [];
    if (ctx.query.chapter) {
        let chapter_id = ctx.query.chapter;
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
    }
    ctx.body = ret.sort((a, b) => a.id > b.id ? 1 : -1);
}).get("/getChapterProblemsInfo", async ctx => {
    let ret = [];
    if (ctx.query.chapter) {
        let chapter_id = ctx.query.chapter;
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
    }
    ctx.body = ret.sort((a, b) => a.id > b.id ? 1 : -1);
}).post("/getUserBasicInfo", async ctx => {
    const users = ctx.request.body.users;
    const ret = [];
    for (const uid of users) {
        const user = await user_model_old.findOne({ originalId: uid }).select(
            "educationLevel birthDate gender country"
        );
        if (!user) {
            continue;
        }

        ret.push({
            user_id: uid,
            level_of_education: user.educationLevel,
            year_of_birth: user.birthDate,
            gender: user.gender,
            country: user.country,
        });
    }
    ctx.body = ret;
});

async function init() {
    course_modules = await module_model.find();
    course_chapters = course_modules.filter(d => d.category == 'chapter');
}

init();
app.use(cors());
app.use(bodyParser());
app.use(Routers.routes());

app.listen(3000);
