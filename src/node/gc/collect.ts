import {LevelDown as LevelDOWN} from "leveldown";
import {eachSeries, series} from "async";

import {IActiveTopics} from "../pipeProc";
import {IProc} from "../proc";
import {getRange} from "../getRange";
import {transaction} from "../transaction";
import {decrementCurrentTone} from "../commitLog";

export function collect(
    db: LevelDOWN,
    activeTopics: IActiveTopics,
    activeProcs: IProc[],
    options: {minPruneTime: number},
    callback: (err?: Error | null) => void
) {
    if (Object.keys(activeTopics).length === 0) return;
    const topicsWithoutProcs: IActiveTopics = {};
    const topicsWithProcs: IActiveTopics = {};
    const currentTS = Date.now();
    const MIN_PRUNE_TIME = options.minPruneTime;

    //categorize to topicsWithProcs and topicWithoutProcs
    Object.keys(activeTopics).forEach(function(topicName) {
        const topic = activeTopics[topicName];
        let hasProc = false;
        activeProcs.forEach(function(proc) {
            if (hasProc) return;
            if (proc.topic === topicName && proc.status === "active") {
                hasProc = true;
                topicsWithProcs[topicName] = topic;
            }
        });
        if (!hasProc) {
            topicsWithoutProcs[topicName] = topic;
        }
    });

    const toCollect: {
        topic: string,
        toneId: string,
        ts: number
    }[] = [];

    const ZERO_TONE = "0000000000000000";
    const FIRST_TONE = "0000000000000001";

    //for topicsWithProcs find the smallest possible id processed by all of its procs
    Object.keys(topicsWithProcs).forEach(function(topicName) {
        const minProc = activeProcs
            .filter(p => p.topic === topicName && p.status === "active")
            .map(p => {
                if (!p.previousClaimedRange) return {name: p.name, toneId: ZERO_TONE, ts: 0};
                const toneId = p.previousClaimedRange.split("..")[0].split("-")[1];
                const ts = parseInt(p.previousClaimedRange.split("..")[0].split("-")[0]);
                if (toneId === FIRST_TONE) return {name: p.name, toneId: ZERO_TONE, ts: 0};
                return {
                    name: p.name,
                    toneId: decrementCurrentTone(toneId),
                    ts: ts
                };
            })
            .filter(p => p.toneId > ZERO_TONE)
            .reduce((p, v) => (p.toneId < v.toneId && p.toneId > ZERO_TONE ? p : v), {toneId: ZERO_TONE, ts: 0});
        if (minProc.toneId > ZERO_TONE && minProc.ts <= currentTS - MIN_PRUNE_TIME) {
            toCollect.push({
                topic: topicName,
                toneId: minProc.toneId,
                ts: minProc.ts
            });
        }
    });

    series([
        //for topicsWithoutProcs find the actual key based on MIN_PRUNE_TIME
        function(cb) {
            eachSeries(Object.keys(topicsWithoutProcs), function(topicName, next) {
                getRange(
                    db,
                    activeTopics,
                    topicName,
                    (currentTS - MIN_PRUNE_TIME).toString(),
                    "",
                    1,
                    false,
                    true,
                    function(err, logs) {
                        if (err) return next(err);
                        if (logs && logs.length > 0) {
                            toCollect.push({
                                topic: topicName,
                                toneId: logs[0].id.split("-")[1],
                                ts: parseInt(logs[0].id.split("-")[0])
                            });
                        }
                        next();
                    });
            }, cb);
        },
        //get all the keys for the collectables and add them to a transaction
        function(cb) {
            const tx = transaction(db);
            eachSeries(toCollect, function(collectable, nextCollectable) {
                getRange(
                    db,
                    activeTopics,
                    collectable.topic,
                    `${collectable.ts}-${collectable.toneId}`,
                    "",
                    -1,
                    false,
                    true,
                    function(err, logs) {
                        if (err) return nextCollectable(err);
                        if (logs && logs.length > 0) {
                            logs.forEach(function(log) {
                                tx.add([
                                    {key: `topic#${collectable.topic}#key#${log.id}`},
                                    {key: `~~internal~~#topic#${collectable.topic}#idKey#${log.id.split("-")[1]}`}
                                ]);
                            });
                        }
                        nextCollectable();
                    });
            }, function(err) {
                if (err) return cb(err);
                tx.commitDelete(cb);
            });
        }
        // the gc is incomplete, expired procs, systemProcs and topic metadata are never collected
        // ,
        // function(cb) {
        //     const expiredProcs = activeProcs.filter(p => {
        //         return p.lastAckedRange &&
        //             parseInt(p.lastAckedRange.split("..")[1].split("-")[0]) < currentTS - MIN_PRUNE_TIME;
        //     });
        //     if (expiredProcs.length === 0) return asyncImmediate(cb);
        //     eachSeries(expiredProcs, function(expiredProc, nextExpiredProc) {
        //         getRange(
        //             db,
        //             activeTopics,
        //             expiredProc.topic,
        //             "",
        //             "",
        //             1,
        //             true,
        //             false,
        //             function(err, logs) {
        //                 if (err) return nextExpiredProc(err);
        //                 if (!logs || logs.length === 0) {
        //                     destroyProc(db, activeProcs, expiredProc.name, function(destroyErr) {
        //                         if (destroyErr) return nextExpiredProc(destroyErr);
        //                     });
        //                 }
        //             });
        //     });
        // }
    ], function(err) {
        if (err) return callback(<Error>err);
        callback();
    });

}
