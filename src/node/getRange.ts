import debug from "debug";
import {LevelDown as LevelDOWN, Bytes} from "leveldown";
import {forever, series, setImmediate as asyncImmediate} from "async";
import {IActiveTopics} from "./pipeProc";
import {zeroPad} from "./tones";

const d = debug("pipeproc:node");
const VALID_RANGE_INPUT = /^(([0-9]*)|(:{0,1}[0-9]+)|([0-9]+-[0-9]+))$/;

export interface IRangeResult {
    id: string;
    data: string;
}

export interface IRangeIteratorOptions {
    [index: string]: Bytes | number | boolean | undefined;
    "gt"?: string;
    "lt"?: string;
    "gte"?: string;
    "lte"?: string;
    limit?: number;
    keyAsBuffer: false;
    valueAsBuffer: false;
    reverse: boolean;
}

type Sdo = {isStartIdSearch: boolean, isEndIdSearch: boolean, startId: Bytes, endId: Bytes};

export function getRange(
    db: LevelDOWN,
    activeTopics: IActiveTopics,
    topic: string,
    start: string,
    end: string,
    limit: number,
    exclusive: boolean,
    reverse: boolean,
    callback: (err?: Error | null, results?: IRangeResult[]) => void
): void {
    if (!activeTopics[topic]) {
        return callback(new Error("invalid_topic"));
    }
    if (start) {
        if (!start.match(VALID_RANGE_INPUT)) {
            return callback(new Error("invalid_range_offset"));
        }
    }
    if (end) {
        if (!end.match(VALID_RANGE_INPUT)) {
            return callback(new Error("invalid_range_offset"));
        }
    }
    const prefix = `topic#${topic}#key#`;
    const sdo: Sdo = {isStartIdSearch: false, isEndIdSearch: false, startId: "", endId: ""};
    const results: IRangeResult[] = [];
    series([
        function(cb) {
            if (start.match(/:[0-9]+/)) {
                sdo.isStartIdSearch = true;
                let parsed = start.replace(":", "");
                parsed = zeroPad(parsed);
                const idKey = `~~internal~~#topic#${topic}#idKey#${parsed}`;
                d("doing id search for start, idKey:", idKey);
                db.get(idKey, {asBuffer: false}, function(err, value) {
                    if (err && err.message.indexOf("NotFound") > -1) {
                        cb(new Error("invalid_tone_id_search"));
                    } else if (err) {
                        cb(err);
                    } else {
                        d("startKey:", value);
                        sdo.startId = value;
                        cb();
                    }
                });
            } else {
                cb();
            }
        },
        function(cb) {
            if (end.match(/:[0-9]+/)) {
                sdo.isEndIdSearch = true;
                let parsed = end.replace(":", "");
                parsed = zeroPad(parsed);
                const idKey = `~~internal~~#topic#${topic}#idKey#${parsed}`;
                d("doing id search for end, idKey:", idKey);
                db.get(idKey, {asBuffer: false}, function(err, value) {
                    if (err && err.message.indexOf("NotFound") > -1) {
                        cb(new Error("invalid_tone_id_search"));
                    } else if (err) {
                        cb(err);
                    } else {
                        d("endKey:", value);
                        sdo.endId = value;
                        cb();
                    }
                });
            } else {
                cb();
            }
        },
        function(cb) {
            const iteratorOptions = getIteratorOptions(
                prefix,
                start,
                end,
                limit,
                reverse,
                exclusive,
                sdo
            );
            const iterator = db.iterator(iteratorOptions);
            forever(function(next) {
                iterator.next(function(err, key, value) {
                    if (err) return next(err);
                    if (!key) return next(new Error("stop"));
                    if (key.indexOf(prefix) > -1) {
                        results.push({id: key.toString().split(prefix)[1], data: value.toString()});
                    }
                    asyncImmediate(next);
                });
            }, function(status) {
                if (!status || status.message === "stop") {
                    iterator.end(cb);
                } else {
                    cb(status);
                }
            });
        }
    ], function(err) {
        if (err) {
            callback(err);
        } else {
            d("range results: \n%O", results);
            callback(null, results);
        }
    });
}

function getIteratorOptions(
    prefix: string,
    start: string,
    end: string,
    limit: number,
    reverse: boolean,
    exclusive: boolean,
    sdo: Sdo
): IRangeIteratorOptions {
    const iteratorOptions: IRangeIteratorOptions = {
        keyAsBuffer: false,
        valueAsBuffer: false,
        reverse: reverse
    };
    const comparator = {begin: "gte", end: "lte"};
    if (exclusive) {
        comparator.begin = "gt";
        comparator.end = "lt";
    }
    if (reverse) {
        if (sdo.isStartIdSearch) {
            iteratorOptions[comparator.end] = sdo.startId;
        } else {
            if (start && start.indexOf(":") === -1) {
                iteratorOptions[comparator.end] = prefix + start;
                if (!(iteratorOptions[comparator.end] || "").toString().match(/-.+|-[0-9]+/)) {
                    iteratorOptions[comparator.end] += "~";
                }
            } else {
                iteratorOptions[comparator.end] = `${prefix}~`;
            }
        }
        if (sdo.isEndIdSearch) {
            iteratorOptions[comparator.begin] = sdo.endId;
        } else {
            if (end && end.indexOf(":") === -1) {
                iteratorOptions[comparator.begin] = prefix + end;
            } else {
                iteratorOptions[comparator.begin] = `${prefix} `;
            }
        }
    } else {
        if (sdo.isStartIdSearch) {
            iteratorOptions[comparator.begin] = sdo.startId;
        } else {
            if (start && start.indexOf(":") === -1) {
                iteratorOptions[comparator.begin] = prefix + start;
            } else {
                iteratorOptions[comparator.begin] = `${prefix} `;
            }
        }
        if (sdo.isEndIdSearch) {
            iteratorOptions[comparator.end] = sdo.endId;
        } else {
            if (end && end.indexOf(":") === -1) {
                    iteratorOptions[comparator.end] = prefix + end;
                    if (!(iteratorOptions[comparator.end] || "").toString().match(/-.+|-[0-9]+/)) {
                        iteratorOptions[comparator.end] += "~";
                    }
            } else {
                iteratorOptions[comparator.end] = `${prefix}~`;
            }
        }
    }
    if (limit && limit > 0) {
        iteratorOptions.limit = limit;
    }

    if (iteratorOptions.reverse) {
        d("iterating:", iteratorOptions[comparator.end], "=>", iteratorOptions[comparator.begin]);
    } else {
        d("iterating:", iteratorOptions[comparator.begin], "=>", iteratorOptions[comparator.end]);
    }

    return iteratorOptions;
}
