//tslint:disable
import "jest-extended";
//tslint:enable
import {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../src/node/commitLog";
import {proc, IProc} from "../../src/node/proc";
import {ack} from "../../src/node/ack";
import {IActiveTopics} from "../../src/node/pipeProc";

describe("with a '>' offset", function() {
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
    it("should return the very first log on the first run", function(done) {
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
                expect(log).toBeDefined();
                if (!Array.isArray(log)) {
                    expect(log.id).toBe(addedIds[0]);
                    done();
                } else {
                    done.fail("invalid_proc_result");
                }
            }
        });
    });
    it("should be able to return multiple logs at a time", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 3,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                expect(log).toBeDefined();
                expect(log).toBeArray();
                expect(log).toHaveLength(3);
                if (Array.isArray(log)) {
                    expect(log[0].id).toBe(addedIds[0]);
                    expect(log[1].id).toBe(addedIds[1]);
                    expect(log[2].id).toBe(addedIds[2]);
                    done();
                } else {
                    done.fail("invalid_proc_result");
                }
            }
        });
    });
    it("should have the lastClaimedRange equal to the log range when returning multiple logs", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 3,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                expect(activeProcs.find(p => {
                    return p.name === "my_proc_0";
                }).lastClaimedRange).toBe(`${addedIds[0]}..${addedIds[2]}`);
                done();
            }
        });
    });
    it("should not return a log if it is called twice without ack", function(done) {
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
                        expect(log2).toBeUndefined();
                        done();
                    }
                });
            }
        });
    });
    it("ack should return the range acked", function(done) {
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
                ack(db, activeProcs, "my_proc_0", function(ackErr, ackedLogId) {
                    if (ackErr) {
                        done.fail(ackErr);
                    } else {
                        expect(ackedLogId).toBe(`${addedIds[0]}..${addedIds[0]}`);
                        done();
                    }
                });
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
                ack(db, activeProcs, "my_proc_0", function(ackErr, ackedLogId) {
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
    it("should return an error if procName is not unique", function(done) {
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
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc_0",
                    topic: "my_topic_1",
                    offset: ">",
                    count: 1,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(err2, log2) {
                    expect(err2).toBeInstanceOf(Error);
                    expect(err2.message).toBe("proc_name_not_unique");
                    expect(log2).toBeUndefined();
                    done();
                });
            }
        });
    });
    it("should return an error if something goes wrong with leveldb", function(done) {
        //@ts-ignore
        db.batch = jest.fn(function(_, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            expect(err).toBeInstanceOf(Error);
            expect(log).toBeUndefined();
            done();
        });
    });
    it("should return an error if something goes wrong with getRange()", function(done) {
        //@ts-ignore
        db.iterator = jest.fn(function() {
            return {
                next: function(callback) {
                    //@ts-ignore
                    callback(new Error("Range Error"));
                }
            };
        });
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            expect(err).toBeInstanceOf(Error);
            expect(log).toBeUndefined();
            expect(err.message).toBe("Range Error");
            done();
        });
    });
    it("should return an error if something goes wrong with leveldb on the second run", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err) {
            if (err) {
                done.fail(err);
            }
            ack(db, activeProcs, "my_proc_0", function(ackErr, ackedLogId) {
                if (ackErr) {
                    done.fail(ackErr);
                } else {
                    //@ts-ignore
                    db.batch = jest.fn(function(_, callback) {
                        //@ts-ignore
                        callback(new Error("Commit Error"));
                    });
                    proc(db, activeProcs, activeTopics, {
                        name: "my_proc_0",
                        topic: "my_topic_0",
                        offset: ">",
                        count: 1,
                        maxReclaims: 10,
                        reclaimTimeout: 10000,
                        onMaxReclaimsReached: "disable"
                    }, function(err2, log2) {
                        expect(err2).toBeInstanceOf(Error);
                        expect(log2).toBeUndefined();
                        done();
                    });
                }
            });
        });
    });
});

describe("with a '$>' offset", function() {
    let db: LevelDOWN;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let addedIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                addedIds = ids;
                done();
            } else {
                done.fail("Invalid logs created");
            }
        });
    });
    it("should not return the very first log on the first run", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: "$>",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                expect(log).toBeUndefined();
                done();
            }
        });
    });
    it("should return the first log after the proc's creation", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: "$>",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr, log) {
            if (procErr) {
                done.fail(procErr);
            } else {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": \"new data\"}"
                }, function(commitError, theId) {
                    if (commitError) {
                        done.fail(commitError);
                    } else {
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: "$>",
                            count: 1,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(procErr2, log2) {
                            if (procErr2) {
                                done.fail(procErr2);
                            } else {
                                if (!Array.isArray(log2)) {
                                    expect(log2.id).toBe(theId);
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
    it("should not return a log if it is called twice without ack", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: "$>",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr, log) {
            if (procErr) {
                done.fail(procErr);
            } else {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": \"new data\"}"
                }, function(commitError, theId) {
                    if (commitError) {
                        done.fail(commitError);
                    } else {
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: "$>",
                            count: 1,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(procErr2, log2) {
                            if (procErr2) {
                                done.fail(procErr2);
                            } else {
                                proc(db, activeProcs, activeTopics, {
                                    name: "my_proc_0",
                                    topic: "my_topic_0",
                                    offset: "$>",
                                    count: 1,
                                    maxReclaims: 10,
                                    reclaimTimeout: 10000,
                                    onMaxReclaimsReached: "disable"
                                }, function(procErr3, log3) {
                                    if (procErr3) {
                                        done.fail(procErr3);
                                    } else {
                                        expect(log3).toBeUndefined();
                                        done();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    });
    it("should return the second log after acking the first one", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: "$>",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr, log) {
            if (procErr) {
                done.fail(procErr);
            } else {
                commitLog(db, activeTopics, [{
                    topic: "my_topic_0",
                    body: "{\"myData\": \"new data\"}"
                }, {
                    topic: "my_topic_0",
                    body: "{\"myData\": \"new data\"}"
                }], function(commitError, newIds) {
                    if (commitError) {
                        done.fail(commitError);
                    } else {
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: "$>",
                            count: 1,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(procErr2, log2) {
                            if (procErr2) {
                                done.fail(procErr2);
                            } else {
                                ack(db, activeProcs, "my_proc_0", function(ackErr) {
                                    if (ackErr) return done.fail(ackErr);
                                    proc(db, activeProcs, activeTopics, {
                                        name: "my_proc_0",
                                        topic: "my_topic_0",
                                        offset: "$>",
                                        count: 1,
                                        maxReclaims: 10,
                                        reclaimTimeout: 10000,
                                        onMaxReclaimsReached: "disable"
                                    }, function(procErr3, log3) {
                                        if (procErr3) {
                                            done.fail(procErr3);
                                        } else {
                                            if (!Array.isArray(log3)) {
                                                expect(log3.id).toBe(newIds[1]);
                                                done();
                                            } else {
                                                done.fail("invalid_proc_result");
                                            }
                                        }
                                    });
                                });
                            }
                        });
                    }
                });
            }
        });
    });
});

describe("with a numeric offset", function() {
    let db: LevelDOWN;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let addedIds: string[];
    let numericOffset: string;
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
                    numericOffset = ":1";
                    done();
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });
    it("should return the correct log on the first run", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: numericOffset,
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                if (!Array.isArray(log)) {
                    expect(log.id).toBe(addedIds[1]);
                    done();
                } else {
                    done.fail("invalid_proc_result");
                }
            }
        });
    });
    it("should not return a log if it is called twice without ack", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: numericOffset,
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc_0",
                    topic: "my_topic_0",
                    offset: numericOffset,
                    count: 1,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(err2, log2) {
                    if (err) {
                        done.fail(err);
                    } else {
                        expect(log2).toBeUndefined();
                        done();
                    }
                });
            }
        });
    });
    it("should return the second log after acking the first one", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: numericOffset,
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, log) {
            if (err) {
                done.fail(err);
            } else {
                ack(db, activeProcs, "my_proc_0", function(ackErr, ackedLogId) {
                    if (ackErr) {
                        done.fail(ackErr);
                    } else {
                        proc(db, activeProcs, activeTopics, {
                            name: "my_proc_0",
                            topic: "my_topic_0",
                            offset: numericOffset,
                            count: 1,
                            maxReclaims: 10,
                            reclaimTimeout: 10000,
                            onMaxReclaimsReached: "disable"
                        }, function(err2, log2) {
                            if (err) {
                                done.fail(err);
                            } else {
                                if (!Array.isArray(log2)) {
                                    expect(log2.id).toBe(addedIds[2]);
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
