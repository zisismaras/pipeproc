//tslint:disable
import "jest-extended";
//tslint:enable
import {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../src/node/commitLog";
import {proc, IProc} from "../../src/node/proc";
import {ackCommitLog} from "../../src/node/ackCommitLog";
import {IActiveTopics} from "../../src/node/pipeProc";

describe("add a single log", function() {
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
                    done();
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });
    it("should return the second log after acking the first one", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                ackCommitLog(
                    db, activeTopics, activeProcs, "my_proc_0",
                    {topic: "a_random_topic", body: "{\"theBestData\": 0}"},
                function(ackErr, status) {
                    if (ackErr) {
                        done.fail(ackErr);
                    } else {
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: ">",
                            count: 1,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(err2, log2) {
                            if (err) {
                                done.fail(err);
                            } else {
                                if (!Array.isArray(log2)) {
                                    expect(log2).toBeDefined();
                                    expect(log2.id).toBe(addedIds[1]);
                                    done();
                                } else {
                                    done.fail("invalid_proc_result");
                                }
                            }
                        });
                    }
                });
            }
        });
    });
    it("should return the acked range and the log added", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                ackCommitLog(
                    db, activeTopics, activeProcs, "my_proc_0",
                    {topic: "a_random_topic", body: "{\"theBestData\": 0}"},
                function(ackErr, status) {
                    if (ackErr) {
                        done.fail(ackErr);
                    } else {
                        expect(status[0]).toBe(`${addedIds[0]}..${addedIds[0]}`);
                        expect(status[1]).toEndWith("-0");
                        done();
                    }
                });
            }
        });
    });
    it("should return an error if an invalid proc is passed", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                ackCommitLog(
                    db, activeTopics, activeProcs, "uknown_proc",
                    {topic: "a_random_topic", body: "{\"theBestData\": 0}"},
                function(ackErr, status) {
                    expect(ackErr).toBeInstanceOf(Error);
                    expect(ackErr.message).toBe("invalid_proc");
                    expect(status[0]).toBe("");
                    expect(status[1]).toBe("");
                    done();
                });
            }
        });
    });
    it("should return an error if something goes wrong with leveldb", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                //@ts-ignore
                db.batch = jest.fn(function(_, callback) {
                    //@ts-ignore
                    callback(new Error("Commit Error"));
                });
                ackCommitLog(
                    db, activeTopics, activeProcs, "my_proc_0",
                    {topic: "a_random_topic", body: "{\"theBestData\": 0}"},
                function(ackErr, status) {
                    expect(ackErr).toBeInstanceOf(Error);
                    expect(ackErr.message).toBe("Commit Error");
                    expect(status[0]).toBe("");
                    expect(status[1]).toBe("");
                    done();
                });
            }
        });
    });
});

describe("add multiple logs", function() {
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
                    done();
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });
    it("should return the acked range and the logs added", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                ackCommitLog(
                    db, activeTopics, activeProcs, "my_proc_0",
                    [
                        {topic: "a_random_topic", body: "{\"theBestData\": 0}"},
                        {topic: "a_random_topic", body: "{\"theBestData\": 1}"},
                        {topic: "another_topic", body: "{\"theBestData\": 2}"}
                    ],
                function(ackErr, status) {
                    if (ackErr) {
                        done.fail(ackErr);
                    } else {
                        expect(status[0]).toBe(`${addedIds[0]}..${addedIds[0]}`);
                        expect(status[1]).toBeArray();
                        expect(status[1][0]).toEndWith("-0");
                        expect(status[1][1]).toEndWith("-1");
                        expect(status[1][2]).toEndWith("-0");
                        done();
                    }
                });
            }
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
