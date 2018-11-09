//tslint:disable
import "jest-extended";
//tslint:enable
import LevelDOWN from "leveldown";
import MemDOWN from "memdown";
import {proc, IProc} from "../../src/node/proc";
import {IActiveTopics} from "../../src/node/pipeProc";
import {destroyProc} from "../../src/node/destroyProc";
import {commitLog} from "../../src/node/commitLog";

let db: LevelDOWN.LevelDown;
let activeTopics: IActiveTopics;
let activeProcs: IProc[];
beforeEach(function(done) {
    db = MemDOWN();
    activeTopics = {};
    activeProcs = [];
    commitLog(db, activeTopics, {
        topic: "my_topic_0",
        body: "{\"myData\": 1}"
    }, function(commitErr) {
        if (commitErr) {
            done.fail(commitErr);
        } else {
            proc(db, activeProcs, activeTopics, {
                name: "my_proc_0",
                topic: "my_topic_0",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable"
            }, function(procErr, log) {
                if (procErr) {
                    done.fail(procErr);
                } else {
                    done();
                }
            });
        }
    });
});
it("should remove the in-memory proc", function(done) {
    destroyProc(db, activeProcs, "my_proc_0", function(err) {
        if (err) {
            done.fail(err);
        } else {
            expect(activeProcs.find(p => p.name === "my_proc_0")).toBeUndefined();
            done();
        }
    });
});
it("should return the in-memory proc", function(done) {
    const procSnapshot: IProc = JSON.parse(JSON.stringify(activeProcs[0]));
    destroyProc(db, activeProcs, "my_proc_0", function(err, theProc) {
        if (err) {
            done.fail(err);
        } else {
            expect(theProc.name).toBe(procSnapshot.name);
            done();
        }
    });
});
it("should leave no keys behind", function(done) {
    destroyProc(db, activeProcs, "my_proc_0", function(err) {
        if (err) {
            done.fail(err);
        } else {
            const iterator = db.iterator({
                gte: "~~system~~#proc#$my_topic_0#$my_proc_0#",
                values: false,
                limit: -1
            });
            iterator.next(function(iteratorErr, key) {
                if (iteratorErr) {
                    done.fail(err);
                } else {
                    expect(key).toBeUndefined();
                    done();
                }
            });
        }
    });
});
it("should return an error if an invalid proc is passed", function(done) {
    destroyProc(db, activeProcs, "uknown_proc", function(err, theProc) {
        expect(err).toBeInstanceOf(Error);
        expect(err.message).toBe("invalid_proc");
        expect(theProc).toBeUndefined();
        done();
    });
});
it("should return an error if something goes wrong with the iterator", function(done) {
    db.iterator = jest.fn(function() {
        return {
            next: function(callback) {
                callback(new Error("Range Error"));
            }
        };
    });
    destroyProc(db, activeProcs, "my_proc_0", function(err, theProc) {
        expect(err).toBeInstanceOf(Error);
        expect(err.message).toBe("Range Error");
        expect(theProc).toBeUndefined();
        done();
    });
});
it("should return an error if something goes wrong with the iterator's end", function(done) {
    db.iterator = jest.fn(function() {
        return {
            next: function(callback) {
                callback();
            },
            end: function(callback) {
                callback(new Error("Iterator End Error"));
            }
        };
    });
    destroyProc(db, activeProcs, "my_proc_0", function(err, theProc) {
        expect(err).toBeInstanceOf(Error);
        expect(err.message).toBe("Iterator End Error");
        expect(theProc).toBeUndefined();
        done();
    });
});
it("should return an error if something goes wrong with leveldb", function(done) {
    db.batch = jest.fn(function(_, callback) {
        callback(new Error("Commit Error"));
    });
    destroyProc(db, activeProcs, "my_proc_0", function(err, theProc) {
        expect(err).toBeInstanceOf(Error);
        expect(err.message).toBe("Commit Error");
        expect(theProc).toBeUndefined();
        done();
    });
});
