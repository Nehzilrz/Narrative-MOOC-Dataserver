const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const COURSES = 'courses';
const CourseSchema = new Schema({
    originalId: String,
    name: String,
    year: Number,
    org: String,
    courseImageUrl: String,
    instructor: [String], // missing
    status: String, // missing
    url: String, // missing
    description: String,
    startDate: Number, // the microsecond of date(use date.gettime())
    endDate: Number, // the microsecond of date(use date.gettime())
    enrollmentStart: Number,
    enrollmentEnd: Number,
    studentIds: [String],
    videoIds: [String],
    grades: Schema.Types.Mixed,
    metaInfo: String,
}, { collection: COURSES });

const USERS = 'users';
const UserSchema = new Schema({
    originalId: String,
    username: String,
    name: String,
    language: String,
    location: String,
    birthDate: Number,
    educationLevel: String,
    bio: String,
    gender: String,
    country: String,
    activeness: Schema.Types.Mixed,
    courseRoles: Schema.Types.Mixed,
    courseIds: [String],
    droppedCourseIds: [String],
}, { collection: USERS });

const ENROLLMENTS = 'enrollments';
const EnrollmentSchema = new Schema({
    userId: String,
    courseId: String,
    timestamp: Number, // the microsecond of date(use date.gettime())
    action: String,
}, { collection: ENROLLMENTS });

const VIDEOS = 'videos';
const VideoSchema = new Schema({
    originalId: String,
    name: String,
    temporalHotness: Schema.Types.Mixed,
    section: String,
    description: String,
    releaseDate: Number, // the microsecond of date(use date.gettime())
    url: String,
    duration: Number,
    metaInfo: Schema.Types.Mixed,
}, { collection: VIDEOS });

const FORUM = 'forumthreads';
const ForumSchema = new Schema({
    authorId: String,
    originalId: String,
    courseId: String,
    createdAt: Number,
    updatedAt: Number,
    body: String,
    sentiment: {
        type: Number,
        max: 1,
        min: -1,
    },
    type: {
        type: String,
        enum: ['CommentThread', 'Comment', null],
    },
    title: String,
    threadType: {
        type: String,
        enum: ['Question', 'Discussion', null],
    },
    commentThreadId: String,
    parentId: String,
}, { collection: FORUM });

const SOCIALNETWORK = 'forumsocialnetworks';
const SocialNetworkSchema = new Schema({
    courseId: String,
    socialNetwork: [Schema.Types.Mixed],
    activeness: Schema.Types.Mixed,
    activenessRange: [Number],
}, { collection: SOCIALNETWORK });

const LOGS = 'logs';
const LogsSchema = new Schema({
    metaInfo: Schema.Types.Mixed,
    userId: String,
    videoId: String,
    courseId: String,
    timestamp: Number, // Date.gettime
    type: String,
}, { collection: LOGS });

const DENSELOGS = 'denselogs';
const DenseLogsSchema = new Schema({
    videoId: String,
    courseId: String,
    timestamp: Number,
    clicks: [Schema.Types.Mixed],
}, { collection: DENSELOGS });

const METADBFILES = 'metadbfiles';
const MetadbFilesSchema = new Schema({
    createdAt: Number,
    lastModified: Number,
    processed: Boolean,
    etag: String,
    path: String,
    type: String,
}, { collection: METADBFILES });

module.exports = {
    COURSES, 
    CourseSchema,
    USERS,
    UserSchema,
    VIDEOS,
    VideoSchema,
    ENROLLMENTS,
    EnrollmentSchema,
    FORUM,
    ForumSchema,
    SOCIALNETWORK,
    SocialNetworkSchema,
    LOGS,
    LogsSchema,
    DENSELOGS,
    DenseLogsSchema,
    METADBFILES,
    MetadbFilesSchema,
}
