import debug from "debug";
import LevelDOWN from "leveldown";
import {transaction} from "./transaction";
import {IActiveTopics} from "./pipeProc";
import {getRange, IRangeResult} from "./getRange";
import {reclaimProc} from "./reclaimProc";

const d = debug("pipeproc:node");

export interface IProc {
    [index: string]: string | number | undefined;
    name: string;
    topic: string;
    status: string;
    createdAt: number;
    lastClaimedRange: string;
    previousClaimedRange: string;
    lastAckedRange: string;
    lastClaimedAt: number;
    lastAckedAt: number;
    offset: string;
    reclaims: number;
    maxReclaims: number;
    reclaimTimeout: number;
    onMaxReclaimsReached: string;
}

export function proc(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    activeTopics: IActiveTopics,
    options: {
        name: string,
        topic: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string
    },
    callback: (err?: Error|null, log?: IRangeResult | IRangeResult[]) => void
): void {
    getProc(db, activeProcs, options, function(procErr, myProc) {
        if (procErr) return callback(procErr);
        if (!myProc) return callback(new Error("invalid_proc"));
        if (myProc.status === "disabled") return callback(new Error("proc_is_disabled"));
        checkClaimTimeout(db, activeProcs, myProc, function(err) {
            if (err) return callback(err);
            if (myProc.status === "disabled") return callback(new Error("proc_is_disabled"));
            if (!activeTopics[options.topic]) {
                return callback();
            }
            getIteratorResult(db, myProc, activeTopics, options.count, function(iteratorErr, result) {
                if (iteratorErr) return callback(iteratorErr);
                if (result) {
                    updateProc(db, myProc, result).commitUpdate(function(updateErr, range, claimedTimestamp) {
                        if (updateErr) return callback(updateErr);
                        if (range && claimedTimestamp) {
                            myProc.previousClaimedRange = myProc.lastClaimedRange;
                            myProc.lastClaimedRange = <string>range;
                            myProc.lastClaimedAt = <number>claimedTimestamp;
                            callback(null, result);
                        } else {
                            callback(new Error("proc_update_failed"));
                        }
                    });
                } else {
                    callback();
                }
            });
        });
    });
}

function getProc(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    options: {
        name: string,
        topic: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string
    },
    callback: (err: Error|null, proc?: IProc) => void
): void {
    const myProc = activeProcs.find(p => p.name === options.name);
    if (myProc && myProc.topic === options.topic) {
        callback(null, myProc);
    } else if (myProc && myProc.topic !== options.topic) {
        callback(new Error("proc_name_not_unique"));
    } else {
        createProc(db, options).commitUpdate(function(err, newProc) {
            if (err) {
                callback(err);
            } else if (newProc) {
                activeProcs.push(<IProc>newProc);
                callback(null, <IProc>newProc);
            } else {
                callback(new Error("proc_creation_failed"));
            }
        });
    }
}

export function createProc(
    db: LevelDOWN.LevelDown,
    options: {
        name: string,
        topic: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string
    }
) {
    d("creating new proc:", options.name, "for topic:", options.topic, "with offset:", options.offset);
    if (options.onMaxReclaimsReached !== "disable" && options.onMaxReclaimsReached !== "continue") {
        options.onMaxReclaimsReached = "disable";
    }
    const createdAt = Date.now();
    const newProc: IProc = {
        name: options.name,
        topic: options.topic,
        offset: options.offset,
        createdAt: createdAt,
        lastClaimedAt: 0,
        lastAckedAt: 0,
        lastClaimedRange: "",
        lastAckedRange: "",
        previousClaimedRange: "",
        status: "active",
        reclaims: 0,
        maxReclaims: options.maxReclaims || 10,
        reclaimTimeout: options.reclaimTimeout || 10000,
        onMaxReclaimsReached: options.onMaxReclaimsReached
    };
    const prefix = `~~system~~#proc#${newProc.topic}#${newProc.name}#`;

    const tx = transaction<IProc>(db);
    tx.add([{
        key: `${prefix}name`,
        value: `${newProc.name}`
    }, {
        key: `${prefix}topic`,
        value: `${newProc.topic}`
    }, {
        key: `${prefix}createdAt`,
        value: `${newProc.createdAt}`
    }, {
        key: `${prefix}offset`,
        value: `${newProc.offset}`
    }, {
        key: `${prefix}status`,
        value: `${newProc.status}`
    }, {
        key: `${prefix}lastAckedRange`,
        value: ""
    }, {
        key: `${prefix}lastClaimedRange`,
        value: ""
    }, {
        key: `${prefix}previousClaimedRange`,
        value: ""
    }, {
        key: `${prefix}lastAckedAt`,
        value: `${newProc.lastAckedAt}`
    }, {
        key: `${prefix}lastClaimedAt`,
        value: `${newProc.lastClaimedAt}`
    }, {
        key: `${prefix}reclaims`,
        value: `${newProc.reclaims}`
    }, {
        key: `${prefix}maxReclaims`,
        value: `${newProc.maxReclaims}`
    }, {
        key: `${prefix}onMaxReclaimsReached`,
        value: `${newProc.onMaxReclaimsReached}`
    }, {
        key: `${prefix}reclaimTimeout`,
        value: `${newProc.reclaimTimeout}`
    }]);

    tx.done(function() {
        return newProc;
    }, "procs");

    return tx;
}

function getIteratorResult(
    db: LevelDOWN.LevelDown,
    myProc: IProc,
    activeTopics: IActiveTopics,
    count: number,
    callback: (err?: Error|null, result?: IRangeResult|IRangeResult[]|null) => void
): void {
    if (myProc.lastAckedRange !== myProc.lastClaimedRange) {
        return callback();
    }
    if (!myProc.topic) return callback(new Error("invalid_proc"));
    let keyOffset: string;
    if (myProc.offset === ">") {
        if (myProc.lastAckedRange && myProc.lastAckedRange.indexOf("..") > -1) {
            const rangeTuple = myProc.lastAckedRange.split("..");
            keyOffset = rangeTuple[1];
        } else {
            keyOffset = "";
        }
    } else if (myProc.offset === "$>") {
        if (myProc.lastAckedRange && myProc.lastAckedRange.indexOf("..") > -1) {
            const rangeTuple = myProc.lastAckedRange.split("..");
            keyOffset = rangeTuple[1];
        } else {
            keyOffset = `${myProc.createdAt}`;
        }
    } else {
        if (myProc.lastAckedRange && myProc.lastAckedRange.indexOf("..") > -1) {
            const rangeTuple = myProc.lastAckedRange.split("..");
            keyOffset = rangeTuple[1];
        } else {
            keyOffset = `${myProc.offset}`;
        }
    }
    d("getting proc result, offset:", keyOffset);
    getRange(
        db,
        activeTopics,
        myProc.topic,
        keyOffset,
        "",
        count || 1,
        true,
        false,
    function(errStatus, results) {
        if (errStatus) {
            callback(new Error(errStatus.message));
        } else {
            if (results && results.length === 1) {
                callback(null, results[0]);
            } else if (results && results.length > 1) {
                callback(null, results);
            } else {
                callback(null, null);
            }
        }
    });
}

function updateProc(
    db: LevelDOWN.LevelDown,
    myProc: IProc,
    result: IRangeResult | IRangeResult[]
) {
    const prefix = `~~system~~#proc#${myProc.topic}#${myProc.name}#`;
    const claimedTimestamp = Date.now();
    let range: string;
    const tx = transaction<string | number>(db);
    if (Array.isArray(result)) {
        const lastResult = result[result.length - 1];
        const firstResult = result[0];
        range = `${firstResult.id}..${lastResult.id}`;
        tx.add([{
            key: `${prefix}lastClaimedRange`,
            value: range
        }, {
            key: `${prefix}previousClaimedRange`,
            value: myProc.lastClaimedRange || ""
        }, {
            key: `${prefix}lastClaimedAt`,
            value: `${claimedTimestamp}`
        }]);
    } else {
        range = `${result.id}..${result.id}`;
        tx.add([{
            key: `${prefix}lastClaimedRange`,
            value: range
        }, {
            key: `${prefix}previousClaimedRange`,
            value: myProc.lastClaimedRange || ""
        }, {
            key: `${prefix}lastClaimedAt`,
            value: `${claimedTimestamp}`
        }]);
    }
    tx.done(function() {
        return range;
    }, "range");
    tx.done(function() {
        return claimedTimestamp;
    }, "claimedTimestamp");

    return tx;
}

function checkClaimTimeout(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    myProc: IProc,
    callback: (err?: Error | null) => void
): void {
    if (myProc.reclaimTimeout !== -1 && myProc.lastClaimedAt > 0 && myProc.lastClaimedRange !== myProc.lastAckedRange &&
        (Date.now() - myProc.lastClaimedAt) >= myProc.reclaimTimeout) {
        d(myProc);
        d("claim timed out, reclaiming proc...");
        reclaimProc(db, activeProcs, myProc.name, function(err) {
            if (err) {
                callback(err);
            } else {
                callback();
            }
        });
    } else {
        callback();
    }
}
