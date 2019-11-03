//tslint:disable
import "jest-extended";
//tslint:enable
import {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../src/node/commitLog";
import {proc, IProc} from "../../src/node/proc";
import {disableProc, resumeProc} from "../../src/node/resumeDisableProc";
import {IActiveTopics} from "../../src/node/pipeProc";

describe("disable procs", function() {
    let db: LevelDOWN;
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

    it("should disable the proc", function(done) {
        disableProc(db, activeProcs, "my_proc_0", function(err, myProc) {
            if (err) return done.fail(err);
            expect(myProc).toBe(activeProcs[0]);
            expect(myProc.status).toBe("disabled");
            done();
        });
    });
    it("should return an error if the proc does not exist", function(done) {
        disableProc(db, activeProcs, "uknown_proc", function(err, myProc) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toBe("invalid_proc");
            expect(myProc).toBeUndefined();
            done();
        });
    });
    it("should return an error if the proc is already disabled", function(done) {
        disableProc(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            disableProc(db, activeProcs, "my_proc_0", function(err2, myProc) {
                expect(err2).toBeInstanceOf(Error);
                expect(err2.message).toBe("proc_already_disabled");
                expect(myProc).toBeUndefined();
                done();
            });
        });
    });
    it("should an error if a disabled proc is used", function(done) {
        disableProc(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            proc(db, activeProcs, activeTopics, {
                name: "my_proc_0",
                topic: "my_topic_0",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable"
            }, function(procError, logs) {
                expect(procError).toBeInstanceOf(Error);
                expect(procError.message).toBe("proc_is_disabled");
                expect(logs).toBeUndefined();
                done();
            });
        });
    });
});

describe("resume procs", function() {
    let db: LevelDOWN;
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
                        disableProc(db, activeProcs, "my_proc_0", function(err2, myProc) {
                            if (err2) return done.fail(err2);
                            done();
                        });
                    });
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });

    it("should resume the proc", function(done) {
        resumeProc(db, activeProcs, "my_proc_0", function(err, myProc) {
            if (err) return done.fail(err);
            expect(myProc).toBe(activeProcs[0]);
            expect(myProc.status).toBe("active");
            expect(myProc.reclaims).toBe(0);
            done();
        });
    });
    it("should return an error if the proc does not exist", function(done) {
        resumeProc(db, activeProcs, "uknown_proc", function(err, myProc) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toBe("invalid_proc");
            expect(myProc).toBeUndefined();
            done();
        });
    });
    it("should return an error if the proc is already active", function(done) {
        resumeProc(db, activeProcs, "my_proc_0", function(err) {
            if (err) return done.fail(err);
            resumeProc(db, activeProcs, "my_proc_0", function(err2, myProc) {
                expect(err2).toBeInstanceOf(Error);
                expect(err2.message).toBe("proc_already_active");
                expect(myProc).toBeUndefined();
                done();
            });
        });
    });
});

function addLogsToTopic(
    db: LevelDOWN,
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
