import debug from "debug";
import LevelDOWN from "leveldown";
import {forever, series} from "async";
import {IActiveTopics, ISystemState} from "./pipeProc";
import {IProc} from "./proc";
import {ISystemProc} from "./systemProc";

const d = debug("pipeproc:node");

export function restoreState(
    db: LevelDOWN.LevelDown,
    activeTopics: IActiveTopics,
    systemState: ISystemState,
    activeProcs: IProc[],
    activeSystemProcs: ISystemProc[],
    inMemory = false,
    callback: (err: {message?: string} | null) => void
): void {
    series([
        function(cb) {
            d("opening database...");
            db.open(cb);
        },
        function(cb) {
            if (inMemory) return cb();
            d("restoring active topics...");
            const iteratorOptions: {
                gte: string,
                keyAsBuffer: false,
                valueAsBuffer: false
            } = {
                gte: "~~system~~#activeTopics#",
                keyAsBuffer: false,
                valueAsBuffer: false
            };
            const iterator = db.iterator(iteratorOptions);
            forever(function(next) {
                iterator.next(function(err, key, value) {
                    if (err) return next(err);
                    if (!key) return next(new Error("stop"));
                    const topic = key.split("~~system~~#activeTopics#")[1];
                    if (key.indexOf("~~system~~#activeTopics#") > -1 && topic) {
                        activeTopics[topic] = {
                            currentTone: -1,
                            createdAt: parseInt(value)
                        };
                    }
                    next();
                });
            }, function(status) {
                if (!status || status.message === "stop") {
                    iterator.end(cb);
                } else {
                    iterator.end(function() {
                        cb(status);
                    });
                }
            });
        },
        function(cb) {
            if (inMemory) return cb();
            d("restoring topic tones...");
            const iteratorOptions: {
                gte: string,
                keyAsBuffer: false,
                valueAsBuffer: false
            } = {
                gte: "~~system~~#currentTone#",
                keyAsBuffer: false,
                valueAsBuffer: false
            };
            const iterator = db.iterator(iteratorOptions);
            forever(function(next) {
                iterator.next(function(err, key, value) {
                    if (err) return next(err);
                    if (!key) return next(new Error("stop"));
                    const topic = key.split("~~system~~#currentTone#")[1];
                    if (key.indexOf("~~system~~#currentTone#") > -1 && topic) {
                        activeTopics[topic].currentTone = parseInt(value);
                    }
                    next();
                });
            }, function(status) {
                if (!status || status.message === "stop") {
                    iterator.end(cb);
                } else {
                    iterator.end(function() {
                        cb(status);
                    });
                }
            });
        },
        function(cb) {
            if (inMemory) return cb();
            d("restoring active procs...");
            const iteratorOptions: {
                gte: string,
                keyAsBuffer: false,
                valueAsBuffer: false
            } = {
                gte: "~~system~~#proc#",
                keyAsBuffer: false,
                valueAsBuffer: false
            };
            const iterator = db.iterator(iteratorOptions);
            forever(function(next) {
                iterator.next(function(err, key, value) {
                    if (err) return next(err);
                    if (!key) return next(new Error("stop"));
                    if (key.indexOf("~~system~~#proc#") === -1) return next();
                    const unprefixed = key.split("~~system~~#proc#")[1];
                    const topic = unprefixed.split("#")[0];
                    const procName = unprefixed.split("#")[1];
                    const procProperty = unprefixed.split("#")[2];
                    const myProc = activeProcs.find(p => p.name === procName);
                    let newProc: IProc;
                    if (myProc) {
                        myProc[procProperty] = formatProcProperty(procProperty, value);
                    } else {
                        newProc = {
                            name: procName,
                            topic: topic,
                            offset: ">",
                            createdAt: Date.now(),
                            lastClaimedAt: 0,
                            lastAckedAt: 0,
                            lastClaimedRange: "",
                            lastAckedRange: "",
                            previousClaimedRange: "",
                            status: "active",
                            reclaims: 0,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        };
                        newProc[procProperty] = formatProcProperty(procProperty, value);
                        activeProcs.push(newProc);
                    }
                    next();
                });
            }, function(status) {
                if (!status || status.message === "stop") {
                    iterator.end(cb);
                } else {
                    iterator.end(function() {
                        cb(status);
                    });
                }
            });
        },
        function(cb) {
            if (inMemory) return cb();
            d("restoring active system procs...");
            const iteratorOptions: {
                gte: string,
                keyAsBuffer: false,
                valueAsBuffer: false
            } = {
                gte: "~~system~~#systemProc#",
                keyAsBuffer: false,
                valueAsBuffer: false
            };
            const iterator = db.iterator(iteratorOptions);
            forever(function(next) {
                iterator.next(function(err, key, value) {
                    if (err) return next(err);
                    if (!key) return next(new Error("stop"));
                    if (key.indexOf("~~system~~#systemProc#") === -1) return next();
                    const unprefixed = key.split("~~system~~#systemProc#")[1];
                    const topic = unprefixed.split("#")[0];
                    const procName = unprefixed.split("#")[1];
                    const procProperty = unprefixed.split("#")[2];
                    const mySystemProc = activeSystemProcs.find(p => p.name === procName);
                    let newSystemProc: ISystemProc;
                    if (mySystemProc) {
                        mySystemProc[procProperty] = value;
                    } else {
                        newSystemProc = {
                            name: procName,
                            topic: topic,
                            to: ""
                        };
                        newSystemProc[procProperty] = value;
                        activeSystemProcs.push(newSystemProc);
                    }
                    next();
                });
            }, function(status) {
                if (!status || status.message === "stop") {
                    iterator.end(cb);
                } else {
                    iterator.end(function() {
                        cb(status);
                    });
                }
            });
        },
        function(cb) {
            if (!inMemory) {
                d("restored topics:", Object.keys(activeTopics));
                d("restored procs:", activeProcs.map(p => p.name));
                d("restored systemProcs:", activeSystemProcs.map(p => p.name));
            }
            systemState.active = true;
            cb();
        }
    ], function(err) {
        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
}

function formatProcProperty(propertyName: string, propertyValue: string): string | number {
    if ([
        "createdAt",
        "lastClaimedAt",
        "lastAckedAt",
        "reclaims",
        "maxReclaims",
        "reclaimTimeout"
    ].indexOf(propertyName) > -1) {
        return parseInt(propertyValue);
    } else {
        return propertyValue;
    }
}
