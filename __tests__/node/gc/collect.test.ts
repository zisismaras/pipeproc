//tslint:disable
import "jest-extended";
//tslint:enable
import {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../../src/node/commitLog";
import {IActiveTopics} from "../../../src/node/pipeProc";
import {proc, IProc} from "../../../src/node/proc";
import {collect} from "../../../src/node/gc/collect";
import {getRange, IRangeResult} from "../../../src/node/getRange";
import {ack} from "../../../src/node/ack";

describe("topics without procs", function() {
    if (process.platform !== "linux") return;
    let db: LevelDOWN;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, 200, function(err) {
            if (err) {
                done.fail(err);
            } else {
                done();
            }
        });
    });
    it("should collect all logs that have passed the MIN_PRUNE_TIME", function(done) {
        collect(db, activeTopics, activeProcs, {minPruneTime: 100}, function(gcErr) {
            if (gcErr) return done.fail(gcErr);
            getAllLogs(db, activeTopics, function(rangeErr, logs) {
                if (rangeErr) return done.fail(rangeErr);
                expect(logs).toBeArrayOfSize(0);
                done();
            });
        });
    });
    it("should not collect logs that have not passed the MIN_PRUNE_TIME", function(done) {
        collect(db, activeTopics, activeProcs, {minPruneTime: 1000}, function(gcErr) {
            if (gcErr) return done.fail(gcErr);
            getAllLogs(db, activeTopics, function(rangeErr, logs) {
                if (rangeErr) return done.fail(rangeErr);
                expect(logs).toBeArrayOfSize(3);
                done();
            });
        });
    });
    it("should collect all logs that have passed the MIN_PRUNE_TIME and not those that haven't", function(done) {
        addLogsToTopic(db, activeTopics, 200, function(commitErr) {
            if (commitErr) return done.fail(commitErr);
            collect(db, activeTopics, activeProcs, {minPruneTime: 300}, function(gcErr) {
                if (gcErr) return done.fail(gcErr);
                getAllLogs(db, activeTopics, function(rangeErr, logs) {
                    if (rangeErr) return done.fail(rangeErr);
                    expect(logs).toBeArrayOfSize(3);
                    done();
                });
            });
        });
    });
});

describe("topics with procs", function() {
    if (process.platform !== "linux") return;
    let db: LevelDOWN;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, 200, function(err) {
            if (err) {
                done.fail(err);
            } else {
                addLogsToTopic(db, activeTopics, 200, function(err2) {
                    if (err2) {
                        done.fail(err2);
                    } else {
                        done();
                    }
                });
            }
        });
    });
    it("should collect all logs before the previousAckedRange that have passed the MIN_PRUNE_TIME", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 2,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            ack(db, activeProcs, "my_proc_0", function(ackErr) {
                if (ackErr) return done.fail(ackErr);
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc_0",
                    topic: "my_topic_0",
                    offset: ">",
                    count: 2,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(procErr2) {
                    if (procErr2) return done.fail(procErr2);
                    ack(db, activeProcs, "my_proc_0", function(ackErr2) {
                        if (ackErr2) return done.fail(ackErr2);
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: ">",
                            count: 2,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(procErr3) {
                            if (procErr3) return done.fail(procErr3);
                            ack(db, activeProcs, "my_proc_0", function(ackErr3) {
                                if (ackErr3) return done.fail(ackErr3);
                                collect(db, activeTopics, activeProcs, {minPruneTime: 300}, function(gcErr) {
                                    if (gcErr) return done.fail(gcErr);
                                    getAllLogs(db, activeTopics, function(rangeErr, logs) {
                                        if (rangeErr) return done.fail(rangeErr);
                                        expect(logs).toBeArrayOfSize(4);
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
    it("should not collect logs before the previousAckedRange that have not passed the MIN_PRUNE_TIME", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 2,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            ack(db, activeProcs, "my_proc_0", function(ackErr) {
                if (ackErr) return done.fail(ackErr);
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc_0",
                    topic: "my_topic_0",
                    offset: ">",
                    count: 2,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(procErr2) {
                    if (procErr2) return done.fail(procErr2);
                    ack(db, activeProcs, "my_proc_0", function(ackErr2) {
                        if (ackErr2) return done.fail(ackErr2);
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: ">",
                            count: 2,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(procErr3) {
                            if (procErr3) return done.fail(procErr3);
                            ack(db, activeProcs, "my_proc_0", function(ackErr3) {
                                if (ackErr3) return done.fail(ackErr3);
                                collect(db, activeTopics, activeProcs, {minPruneTime: 600}, function(gcErr) {
                                    if (gcErr) return done.fail(gcErr);
                                    getAllLogs(db, activeTopics, function(rangeErr, logs) {
                                        if (rangeErr) return done.fail(rangeErr);
                                        expect(logs).toBeArrayOfSize(6);
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
    it("should not collect logs after the previousAckedRange\
        even if they have passed the MIN_PRUNE_TIME", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 2,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            ack(db, activeProcs, "my_proc_0", function(ackErr) {
                if (ackErr) return done.fail(ackErr);
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc_0",
                    topic: "my_topic_0",
                    offset: ">",
                    count: 2,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(procErr2) {
                    if (procErr2) return done.fail(procErr2);
                    ack(db, activeProcs, "my_proc_0", function(ackErr2) {
                        if (ackErr2) return done.fail(ackErr2);
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: ">",
                            count: 2,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(procErr3) {
                            if (procErr3) return done.fail(procErr3);
                            ack(db, activeProcs, "my_proc_0", function(ackErr3) {
                                if (ackErr3) return done.fail(ackErr3);
                                collect(db, activeTopics, activeProcs, {minPruneTime: 100}, function(gcErr) {
                                    if (gcErr) return done.fail(gcErr);
                                    getAllLogs(db, activeTopics, function(rangeErr, logs) {
                                        if (rangeErr) return done.fail(rangeErr);
                                        expect(logs).toBeArrayOfSize(4);
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
    it("should not collect logs if the previousAckedRange is null", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 2,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            ack(db, activeProcs, "my_proc_0", function(ackErr) {
                if (ackErr) return done.fail(ackErr);
                collect(db, activeTopics, activeProcs, {minPruneTime: 100}, function(gcErr) {
                    if (gcErr) return done.fail(gcErr);
                    getAllLogs(db, activeTopics, function(rangeErr, logs) {
                        if (rangeErr) return done.fail(rangeErr);
                        expect(logs).toBeArrayOfSize(6);
                        done();
                    });
                });
            });
        });
    });
    it("should not collect logs if the previousAckedRange is 0", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 2,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            ack(db, activeProcs, "my_proc_0", function(ackErr) {
                if (ackErr) return done.fail(ackErr);
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc_0",
                    topic: "my_topic_0",
                    offset: ">",
                    count: 2,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(procErr2) {
                    if (procErr2) return done.fail(procErr2);
                    ack(db, activeProcs, "my_proc_0", function(ackErr2) {
                        if (ackErr2) return done.fail(ackErr2);
                        collect(db, activeTopics, activeProcs, {minPruneTime: 100}, function(gcErr) {
                            if (gcErr) return done.fail(gcErr);
                            getAllLogs(db, activeTopics, function(rangeErr, logs) {
                                if (rangeErr) return done.fail(rangeErr);
                                expect(logs).toBeArrayOfSize(6);
                                done();
                            });
                        });
                    });
                });
            });
        });
    });
});

describe("noop", function() {
    it("passes", function() {
        expect(1).toBe(1);
    });
});

function getAllLogs(
    db: LevelDOWN,
    activeTopics: IActiveTopics,
    callback: (err?: Error | null, results?: IRangeResult[]) => void) {
    getRange(db, activeTopics, "my_topic_0", "", "", -1, false, false, function(err, results) {
        if (err) return callback(<Error>err);
        callback(null, results);
    });
}

function addLogsToTopic(
    db: LevelDOWN,
    activeTopics: IActiveTopics,
    timeout: number,
    callback: (err: Error|null, ids?: string|string[]) => void)
: void {
    commitLog(db, activeTopics, [{
        topic: "my_topic_0",
        body: "{\"myData\": 1}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 2}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 3}"
    }], function(err, ids) {
        setTimeout(function() {
            callback(err, ids);
        }, timeout);
    });
}
