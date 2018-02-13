const Koa = require('koa');
const Router = require('koa-router')
const mongoose = require('mongoose');
const dbUrl = 'mongodb://localhost/NarrativeMOOCintroduceToJava';
const cors = require('koa2-cors');
const Schema = mongoose.Schema;

const app = new Koa();

mongoose.connect(dbUrl);

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

var currentGrades = null;
var currentCourseId = null;
var memVideoLogPeaks = {};

selectCourse('HKUSTx_COMP102x_2T2014');
async function selectCourse(courseId) {
    const ret = await courses.findOne({ originalId: courseId });
    currentCourseId = courseId;
    currentGrades = ret.grades;
    var maxGrade = 0;
    for (const user in currentGrades) {
        if (currentGrades[user] > maxGrade) {
            maxGrade = currentGrades[user];
        }
    }

    for (const user in currentGrades) {
        currentGrades[user] = ~~(currentGrades[user] * 100 / maxGrade);
    }
    return ret;
}

async function getVideoLogPeaks(videoId, duration) {
    if (memVideoLogPeaks[videoId] != null) {
        return memVideoLogPeaks[videoId];
    }

    const denselogsRet = await denselogs.find({
        videoId: videoId
    });
    const sortedClick = [];
    const actionTypes = {};

    const userFilter = {};
    const userSet = {};
    const videoUserGrades = [];

    var courseId = denselogsRet.length > 0 ? denselogsRet[0].courseId : null;
    if (courseId && courseId != currentCourseId) {
        currentCourseId = courseId;
        await selectCourse(courseId);
    }
    
    for (const denselog of denselogsRet) {
        for (const click of denselog.clicks) {
            const type = click.type;
            const userId = click.userId;
            const currentTime = ~~(click.currentTime || click.oldTime);

            if (userFilter[userId + type] != currentTime) {
                actionTypes[type][currentTime] += 1;
                sortedClick[currentTime].push(click);
                userFilter[userId + type] = currentTime;
            }
            if (!userSet[userId]) {
                userSet[userId] = currentGrades[userId];
                videoUserGrades.push(currentGrades[userId]);
            }
        }
    }
    const entropyAndDistribution = algorithm.getEntropy(videoUserGrades);
    const videoEntropy = entropyAndDistribution[0];
    const videoGradeDistribution = entropyAndDistribution[1];
    const videoGrade = algorithm.getAverage(videoUserGrades);

    const actionLengthThreshold = 30;
    const actionCountThreshold = duration * 5;
    const peaks = {};
    const filteredAction = {};

    for (const action in actionTypes) {
        if (actionTypes[action].length > actionLengthThreshold) {
            filteredAction[action] = algorithm.smooth(actionTypes[action]);
            filteredAction[action][0] = Math.min(filteredAction[action][0], filteredAction[action][1]);
        }

        if (actionTypes[action].length > actionLengthThreshold &&
            actionTypes[action].reduce((a, b) => a + b, 0) > actionCountThreshold) {
            peaks[action] = algorithm.peakDetection(actionTypes[action]);
            for (const peak of peaks[action]) {
                peak.users = [];
                for (var i = Math.max(0, peak.start - 2);
                        i <= Math.min(sortedClick.length - 1, peak.end + 2); ++i) {
                    for (const click of sortedClick[i]) {
                        if (click.type == action && currentGrades[click.userId] > 0) {
                            peak.users.push(click.userId);
                        }
                    }
                }
                peak.users = algorithm.removeDuplicate(peak.users);
                const peakUserGrades = peak.users.map((user) => currentGrades[user]);
                const peakDistribution = algorithm.getEntropy(peakUserGrades);
                const peakEntropy = peakDistribution[0];
                peak.entropyDelta = videoEntropy - peakEntropy;
                peak.avgGrade = algorithm.getAverage(peakUserGrades);
                peak.num = peak.users.length;
                peak.videoId = videoId;
                peak.action = action;
                peak.gradeDistribution = peakDistribution[1];
            }
        }
    }


    memVideoLogPeaks[videoId] = {
        info: {
            entropy: videoEntropy,
            avgGrade: videoGrade,
            gradeDistribution: videoGradeDistribution,
        },
        logs: filteredAction,
        peaks: peaks,
    }
    return memVideoLogPeaks[videoId];
}

const Routers = new Router();
Routers.get("/getCourseList", async ctx => {
    const ret = await courses.find({}).select("_id originalId url startDate endDate name org courseImageUrl instructor");
    ctx.body = ret;
}).get("/selectCourse", async ctx => {
    await selectCourse(ctx.query.courseId);
    ctx.body = true;
}).get("/getVideoList", async ctx => {
    const course = await courses.findOne({
        originalId: ctx.query.courseId
    });
    const ret = [];
    for (var i = 0; i < course.videoIds.length; ++i) {
        var id = course.videoIds[i];
        try {
            const t = await video_model.findOne({ originalId: id });
            ret.push(t);
        } catch (e) {

        }
    }
    ctx.body = ret;
}).get("/getVideoInfo", async ctx => {
    const ret = await video_model.find({
        originalId: ctx.query.videoId
    });
    ctx.body = ret;
}).get("/getCourseLogs", async ctx => {
    const course = await courses.findOne({
        originalId: ctx.query.courseId
    });
    var ret = [];
    for (const videoId of course.videoIds) {
        const videoDuration = (await video_model.findOne({ originalId: videoId })).duration;
        const t = (await getVideoLogPeaks(videoId, videoDuration));
        ret.push({ id: videoId, logs: t.logs, info: t.info });
    }
    ctx.body = ret;
}).get("/getCoursePeaks", async ctx => {
    const course = await courses.findOne({
        originalId: ctx.query.courseId
    });
    var ret = [];
    for (const videoId of course.videoIds) {
        const videoDuration = (await video_model.findOne({ originalId: videoId })).duration;
        const peaks = (await getVideoLogPeaks(videoId, videoDuration)).peaks;
        for (const action in peaks) {
            for (const peak of peaks[action]) {
                ret.push(peak);
            }
        }
    }
    ret = ret.sort((a, b) => b.entropyDelta - a.entropyDelta);
    ctx.body = ret;
}).get("/getVideoLogs", async ctx => {
    const videoId = ctx.query.videoId;
    const videoDuration = (await video_model.findOne({ originalId: videoId })).duration;
    const t = (await getVideoLogPeaks(videoId, videoDuration));
    ctx.body = { id: videoId, logs: t.logs, info: t.info }
}).get("/getVideoPeaks", async ctx => {
    const videoId = ctx.query.videoId;
    const videoDuration = (await video_model.findOne({ originalId: videoId })).duration;
    const peaks = (await getVideoLogPeaks(videoId, videoDuration)).peaks;
    ctx.body = { id: videoId, peaks }
}).get("/getVideoLog", async ctx => {    
    const videoId = ctx.query.videoId;
    const videoDuration = (await video_model.findOne({ originalId: ctx.query.videoId })).duration;
    const ret = await getVideoLogPeaks(ctx.query.videoId, videoDuration);
    ctx.body = ret;
});
app.use(cors());
app.use(Routers.routes());

app.listen(3000);
