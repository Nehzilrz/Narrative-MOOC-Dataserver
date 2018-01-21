
module.exports = {
    peakDetection,
    getEntropy,
    getAverage,
    removeDuplicate,
}

function peakDetection(vec) {
    var pinLength = 1;
    var mean = (vec[0] + vec[1] + vec[2] + vec[3] + vec[4]) / 5;
    var std = Math.abs(vec[0] - mean) + Math.abs(vec[1] - mean) + Math.abs(vec[2] - mean) + Math.abs(vec[3] - mean) + Math.abs(vec[4] - mean);
    const threshold = 3;
    const lag = 5;
    var start = 0;
    var end = 0;
    var diff = 0;
    var alpha = 0.125;
    var result = [];

    for (var i = lag; i < vec.length - lag; i++) {
        if (Math.abs(vec[i] - mean) > threshold * std) {
            start = i;
            while ((i < vec.length) && (vec[i] > vec[i - 1])) {
                diff = Math.abs(mean - vec[i]);
                mean = alpha * vec[i] + (1 - alpha) * mean;
                std = alpha * diff + (1 - alpha) * std;
                i++;
            }
            end = i;
            while ((i < vec.length) && (vec[i] > vec[start])) {
                diff = Math.abs(mean - vec[i]);
                mean = alpha * vec[i] + (1 - alpha) * mean;
                std = alpha * diff + (1 - alpha) * std;
                end = ++i;
            }
            result.push({
                'start': start,
                'end': end,
                'length': end - start + 1,
            });
        } else {
            diff = Math.abs(mean - vec[i]);
            mean = alpha * vec[i] + (1 - alpha) * mean;
            std = alpha * diff + (1 - alpha) * std;
        }
    }
    return result;
}

const quantities = 10, maxValue = 100;
function getEntropy(input) {
    var cnt = [];
    var m = maxValue / quantities;
    for (var i = 0; i < quantities; ++i) {
        cnt.push(0);
    }
    for (var i = 0; i < input.length; ++i) {
        if (input[i] < 100) {
            cnt[~~(input[i] / m)] += 1;
        } else {
            cnt[quantities - 1] += 1;
        }
    }
    var total = input.length;
    var entropy = 0;
    for (var i = 0; i < quantities; ++i) if (cnt[i] != 0) {
        entropy += -Math.log(cnt[i] / total) * cnt[i] / total;
    }
    return [entropy, cnt];
}

function getAverage(input) {
    var ret = 0;
    for (var i = 0; i < input.length; ++i) {
        ret += input[i];
    }
    ret /= input.length;
    return ret;
}


function peakDetectionNew(input) {

    function mean(v, lo, hi) {
        var ret = 0;
        for (var i = lo; i < hi; ++i) {
            ret += v[i];
        }
        return ret / (hi - lo);
    }

    function stdDev(v, lo, hi, avg) {
        var ret = 0;
        for (var i = lo; i < hi; ++i) {
            ret += Math.abs(v[i] - avg);
        }
        return ret;
    }

    const vec = input.slice();
    const lag = 5;
    const threshold = 4;
    var avg = mean(vec, 0, lag);
    var std = stdDev(vec, 0, lag, avg);
    var alpha = 0.3;
    var result = [], n = 0;

    for (var i = lag; i < vec.length - lag; i++) {
        if (vec[i] - avg > threshold * std) {
            if (n != 0 && result[n - 1].end == i - 1) {
                result[n - 1].end = i;
            } else {
                result.push({ start: i, end: i });
                n += 1;
            }
            vec[i] = (1 - alpha) * vec[i] + alpha * vec[i - 1];
        } else {
            diff = Math.abs(avg - vec[i]);
        }
        avg = mean(vec, i - lag + 1, i);
        std = stdDev(vec, i - lag + 1, i, avg);
    }
    return result;
}

function removeDuplicate(vec) {
    vec.sort();
    const output = [vec[0]];
    for (var i = 1; i < vec.length; ++i) {
        if (vec[i] != vec[i - 1]) {
            output.push(vec[i]);
        }
    }
    return output;
}
