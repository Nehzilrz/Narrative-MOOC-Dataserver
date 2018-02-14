const Koa = require('koa');
const Router = require('koa-router')
const mongoose = require('mongoose');
const dbUrl = 'mongodb://localhost/NarrativeMOOCintroduceToJava';
const cors = require('koa2-cors');
const Schema = mongoose.Schema;

const app = new Koa();

mongoose.connect(dbUrl, {
    useMongoClient: true
});

const user_model = mongoose.model('users', new Schema({
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
}));

const event_model = mongoose.model('events', new Schema({
    event_type: String,
    user_id: String,
    module_id: String,
    ip: String,
    session: String,
    event_source: String,
    time: Number,
}));

const video_model = mongoose.model('videos', new Schema({
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

const module_model = mongoose.model('modules', new Schema({
    children: [String],
    category: String,
    id: String,
    display_name: String,
    start: String,
    max_attempts: Number,
    showanswer: String,
    weight: Number,
}));

const video_activies_model = mongoose.model('video_activies', new Schema({
    id: String,
    user_id: String,
    final: Number,
    video_watch_time: Number,
    attempts: Number,
    created: Number,
    modified: Number,
    saved_video_position: String
}));

const problem_activies_model = mongoose.model('problem_activies', new Schema({
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
    const video = await video_model.findOne({ id: videoId });
    cachedVideoLogs[videoId] = Object.keys(video.clickstream)
        .filter((d) => array_is_uniform(video.clickstream[d]))
        .map((d) => ({
            type: d,
            data: array_smooth(video.clickstream[d]),
        }));
    return cachedVideoLogs[videoId];
}

const cachedVideoPeaks = {};
async function getVideoPeaks(videoId) {
    if (cachedVideoPeaks[videoId] != null) {
        return cachedVideoPeaks[videoId];
    }

    const video = await video_model.findOne({ id: videoId });
    const video_peaks = [];

    for (const action in video.peaks) {
        for (const peak of video.peaks[action]) {
            video_peaks.push({
                action: action,
                index: video_peaks.length,
                entropy_delta: peak.entropy - video.entropy,
                start: peak.start,
                end: peak.end,
                length: peak.length,
                activeness: peak.activeness,
                average_grade: peak.average_grade * 100,
                grade_distribution: peak.grade_distribution,
                entropy: peak.entropy,
            })
        }
    }

    cachedVideoPeaks[videoId] = video_peaks;
    return cachedVideoPeaks[videoId];
}

const Routers = new Router();
Routers.get("/getCourseList", async ctx => {
    ctx.body = [];
}).get("/getVideoList", async ctx => {
    const videos = await module_model.find({ category: 'video' });
    let ret = [];
    for (const v of videos) {
        const info = await video_model.findOne({ id : v.id })
            .select('duration grade_distribution average_grade activeness entropy release_date');
        if (!info || info.activeness == 0) {
            continue;
        }
        ret.push({
            name: v.display_name,
            html5_sources: v.html5_sources,
            id: v.id,
            sub: v.sub,
            duration: info.duration,
            grade_distribution: info.grade_distribution,
            average_grade: info.average_grade * 100,
            activeness: info.activeness.action,
            entropy: info.entropy,
            release_date: info.release_date * 1000,
        });
    }
    ctx.body = ret.sort((a, b) => a.name > b.name ? 1 : -1);
}).get("/getProblemList", async ctx => {
    const problems = await module_model.find({ category: 'problem' });
    ctx.body = problems.map(p => ({
        id: p.id,
        name: p.display_name,
        max_attempts: p.max_attempts,
        showanswer: p.showanswer,
        submission_wait_seconds: p.submission_wait_seconds,
        weight: p.weight,
    })).sort((a, b) => a.name > b.name ? 1 : -1);
}).get("/getChapterList", async ctx => {
    const chapters = await module_model.find({ category: 'chapter' });
    ctx.body = chapters.map(c => ({
        name: c.display_name,
        id: c.id,
        start: +(new Date(c.start)),
        problems: c.children.filter(d => d.indexOf('problem') != -1),
        videos: c.children.filter(d => d.indexOf('video') != -1),
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
});

app.use(cors());
app.use(Routers.routes());

app.listen(3000);
