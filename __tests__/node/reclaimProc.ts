//tslint:disable
import "jest-extended";
//tslint:enable
import LevelDOWN from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../src/node/commitLog";
import {proc, IProc} from "../../src/node/proc";
import {ack} from "../../src/node/ack";
import {ackCommitLog} from "../../src/node/ackCommitLog";
import {IActiveTopics} from "../../src/node/pipeProc";
import {reclaimProc} from "../../src/node/reclaimProc";
import {disableProc} from "../../src/node/resumeDisableProc";

describe("manual reclaim", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let addedIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
                done.fail(err);
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    proc(db, activeProcs, activeTopics, {
                        name: "my_proc_0",
                        topic: "my_topic_0",
                        offset: ">",
                        count: 1,
                        maxReclaims: 10,
                        reclaimTimeout: 10000,
                        onMaxReclaimsReached: "disable"
                    }, function(procError) {
                        if (procError) return done.fail(procError);
                        ack(db, activeProcs, "my_proc_0", function(ackError) {
                            if (ackError) return done.fail(ackError);
                            proc(db, activeProcs, activeTopics, {
                                name: "my_proc_0",
                                topic: "my_topic_0",
                                offset: ">",
                                count: 1,
                                maxReclaims: 10,
                                reclaimTimeout: 10000,
                                onMaxReclaimsReached: "disable"
                            }, function(procError2) {
                                if (procError2) return done.fail(procError2);
                                done();
                            });
                        });
                    });
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });

    it("should return the range for the first log", function(done) {
        reclaimProc(db, activeProcs, "my_proc_0", function(err, lastClaimedRange) {
            if (err) return done.fail(err);
            expect(lastClaimedRange).toBe(`${addedIds[0]}..${addedIds[0]}`);
            done();
        });
    });
    it("should get the second log again after a reclaim", function(done) {
        reclaimProc(db, activeProcs, "my_proc_0", function(err, lastClaimedRange) {
            if (err) return done.fail(err);
            proc(db, activeProcs, activeTopics, {
                name: "my_proc_0",
                topic: "my_topic_0",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable"
            }, function(procError, log) {
                if (procError) return done.fail(procError);
                if (Array.isArray(log)) {
                    return done.fail("invalid proc result");
                }
                expect(log.id).toBe(addedIds[1]);
                done();
            });
        });
    });
    it("should disable the proc after maxReclaims have been reached if onMaxReclaimsReached is 'disable'",
    function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 1,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procError, log) {
            if (procError) return done.fail(procError);
            reclaimProc(db, activeProcs, "my_proc_1", function(err, lastClaimedRange) {
                if (err) return done.fail(err);
                const myProc = activeProcs.find(p => p.name === "my_proc_1");
                expect(myProc.reclaims).toBe(1);
                expect(myProc.status).toBe("disabled");
                done();
            });
        });
    });
    it("should continue normally even after maxReclaims have been reached if onMaxReclaimsReached is 'continue'",
    function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 1,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "continue"
        }, function(procError, log) {
            if (procError) return done.fail(procError);
            reclaimProc(db, activeProcs, "my_proc_1", function(err, lastClaimedRange) {
                if (err) return done.fail(err);
                const myProc = activeProcs.find(p => p.name === "my_proc_1");
                expect(myProc.reclaims).toBe(1);
                expect(myProc.status).toBe("active");
                done();
            });
        });
    });
    it("should return an error when an invalid proc is used", function(done) {
        reclaimProc(db, activeProcs, "uknown_proc", function(err, lastClaimedRange) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toBe("invalid_proc");
            done();
        });
    });
    it("should return an error if the proc is disabled", function(done) {
        disableProc(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            reclaimProc(db, activeProcs, "my_proc_0", function(reclaimErr, lastClaimedRange) {
                expect(reclaimErr).toBeInstanceOf(Error);
                expect(reclaimErr.message).toBe("proc_is_disabled");
                done();
            });
        });
    });
    it("should return an if there is nothing to reclaim", function(done) {
        ack(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            reclaimProc(db, activeProcs, "my_proc_0", function(reclaimErr, lastClaimedRange) {
                expect(reclaimErr).toBeInstanceOf(Error);
                expect(reclaimErr.message).toBe("nothing_to_reclaim");
                done();
            });
        });
    });
});

describe("after an ack", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let addedIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
                done.fail(err);
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    proc(db, activeProcs, activeTopics, {
                        name: "my_proc_0",
                        topic: "my_topic_0",
                        offset: ">",
                        count: 1,
                        maxReclaims: 10,
                        reclaimTimeout: 10000,
                        onMaxReclaimsReached: "disable"
                    }, function(procError) {
                        if (procError) return done.fail(procError);
                        done();
                    });
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });

    it("should reset reclaims after a successful ack", function(done) {
        reclaimProc(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            ack(db, activeProcs, "my_proc_0", function(err2) {
                if (err2) return done.fail(err2);
                const myProc = activeProcs.find(p => p.name === "my_proc_0");
                expect(myProc.reclaims).toBe(0);
                done();
            });
        });
    });

    it("should reset reclaims after a successful ackCommit", function(done) {
        reclaimProc(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            ackCommitLog(db, activeTopics, activeProcs, "my_proc_0", {
                topic: "my_topic_0",
                body: "{\"myData\": 5}"
            }, function(err2) {
                if (err2) return done.fail(err2);
                const myProc = activeProcs.find(p => p.name === "my_proc_0");
                expect(myProc.reclaims).toBe(0);
                done();
            });
        });
    });
});

describe("with timeout", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let addedIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
                done.fail(err);
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    proc(db, activeProcs, activeTopics, {
                        name: "my_proc_0",
                        topic: "my_topic_0",
                        offset: ">",
                        count: 1,
                        maxReclaims: 10,
                        reclaimTimeout: 10,
                        onMaxReclaimsReached: "disable"
                    }, function(procError) {
                        if (procError) return done.fail(procError);
                        ack(db, activeProcs, "my_proc_0", function(ackError) {
                            if (ackError) return done.fail(ackError);
                            proc(db, activeProcs, activeTopics, {
                                name: "my_proc_0",
                                topic: "my_topic_0",
                                offset: ">",
                                count: 1,
                                maxReclaims: 10,
                                reclaimTimeout: 10,
                                onMaxReclaimsReached: "disable"
                            }, function(procError2) {
                                if (procError2) return done.fail(procError2);
                                done();
                            });
                        });
                    });
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });

    it("should get the second log again after a reclaimTimeout", function(done) {
        setTimeout(function() {
            proc(db, activeProcs, activeTopics, {
                name: "my_proc_0",
                topic: "my_topic_0",
                offset: ">",
                count: 1,
                maxReclaims: 1,
                reclaimTimeout: 10,
                onMaxReclaimsReached: "disable"
            }, function(procError, log) {
                if (procError) return done.fail(procError);
                if (Array.isArray(log)) {
                    return done.fail("invalid proc result");
                }
                expect(log.id).toBe(addedIds[1]);
                done();
            });
        }, 20);
    });
});

function addLogsToTopic(
    db: LevelDOWN.LevelDown,
    activeTopics: IActiveTopics,
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
    }, {
        topic: "my_topic_1",
        body: "{\"myData\": 1}"
    }, {
        topic: "my_topic_1",
        body: "{\"myData\": 2}"
    }, {
        topic: "my_topic_1",
        body: "{\"myData\": 3}"
    }], function(err, ids) {
        setTimeout(function() {
            callback(err, ids);
        }, 20);
    });
}
