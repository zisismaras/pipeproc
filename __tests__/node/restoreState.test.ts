//tslint:disable
import "jest-extended";
import rimraf from "rimraf";
//tslint:enable
import {tmpdir} from "os";
import LevelDown, {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../src/node/commitLog";
import {restoreState} from "../../src/node/restoreState";
import {proc, IProc} from "../../src/node/proc";
import {IActiveTopics, ISystemState} from "../../src/node/pipeProc";
import {systemProc, ISystemProc} from "../../src/node/systemProc";
import {IWorker} from "../../src/node/workerManager";

describe("system init (disk)", function() {
    const db: LevelDOWN = LevelDown(`${tmpdir()}/pipeproc_test_init`);
    const activeTopics: IActiveTopics = {};
    const activeProcs: IProc[] = [];
    const systemState: ISystemState = {active: false};
    const activeSystemProcs: ISystemProc[] = [];
    const activeWorkers: IWorker[] = [];
    beforeAll(function(done) {
        db.open(function(openErr) {
            if (openErr) return done.fail(openErr);
            addLogsToTopic(db, activeTopics, function(addErr, ids) {
                if (addErr) {
                    return done.fail(addErr);
                }
                proc(db, activeProcs, activeTopics, {
                    name: "my_proc",
                    topic: "my_topic_0",
                    offset: ">",
                    count: 1,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable"
                }, function(procError) {
                    if (procError) return done.fail(procError);
                    systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
                        name: "my_system_proc",
                        from: "my_topic_0",
                        to: "",
                        offset: ">",
                        count: 1,
                        maxReclaims: 10,
                        reclaimTimeout: 10000,
                        onMaxReclaimsReached: "disable",
                        inlineProcessor: "function(log, done) {done()}"
                    }, function(systemProcErr) {
                        if (systemProcErr) return done.fail(systemProcErr);
                        db.close(function(closeErr) {
                            if (closeErr) {
                                return done.fail(closeErr);
                            }
                            done();
                        });
                    });
                });
            });
        });
    });
    afterAll(function(done) {
        db.close(function(closeErr) {
            if (closeErr) {
                return done.fail(closeErr);
            }
            rimraf(`${tmpdir()}/pipeproc_test_init`, function(destroyErr) {
                if (destroyErr) {
                    return done.fail(destroyErr);
                }
                done();
            });
        });
    });
    it("should restore the system state correctly", function(done) {
        const newActiveTopics: IActiveTopics = {};
        const newActiveProcs: IProc[] = [];
        const newActiveSystemProcs: ISystemProc[] = [];
        restoreState(db, newActiveTopics, systemState, newActiveProcs, newActiveSystemProcs, false, function(err) {
            if (err) return done.fail((err && err.message) || "init_error");
            expect(systemState.active).toBeTrue();
            expect(newActiveTopics).toContainKey("my_topic_0");
            expect(newActiveTopics).toContainKey("my_topic_1");
            expect(newActiveTopics.my_topic_0.currentTone).toBe(2);
            expect(newActiveTopics.my_topic_1.currentTone).toBe(2);
            expect(activeProcs.length).toBe(newActiveProcs.length);
            expect(activeProcs[0]).toMatchObject(newActiveProcs[0]);
            expect(activeSystemProcs.length).toBe(newActiveSystemProcs.length);
            expect(activeSystemProcs[0]).toMatchObject(newActiveSystemProcs[0]);
            done();
        });
    });
});

describe("system init (in-memory)", function() {
    //@ts-ignore
    const db: LevelDOWN.LevelDown = MemDOWN();
    const activeTopics: IActiveTopics = {};
    const activeProcs: IProc[] = [];
    const systemState: ISystemState = {active: false};
    const activeSystemProcs: ISystemProc[] = [];
    const activeWorkers: IWorker[] = [];
    beforeAll(function(done) {
        addLogsToTopic(db, activeTopics, function(addErr, ids) {
            if (addErr) {
                return done.fail(addErr);
            }
            proc(db, activeProcs, activeTopics, {
                name: "my_proc",
                topic: "my_topic_0",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable"
            }, function(procError) {
                if (procError) return done.fail(procError);
                systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
                    name: "my_system_proc",
                    from: "my_topic_0",
                    to: "",
                    offset: ">",
                    count: 1,
                    maxReclaims: 10,
                    reclaimTimeout: 10000,
                    onMaxReclaimsReached: "disable",
                    inlineProcessor: "function(log, done) {done()}"
                }, function(systemProcErr) {
                    if (systemProcErr) return done.fail(systemProcErr);
                    db.close(function(closeErr) {
                        if (closeErr) {
                            return done.fail(closeErr);
                        }
                        done();
                    });
                });
            });
        });
    });
    it("should have no state to restore", function(done) {
        const newActiveTopics: IActiveTopics = {};
        const newActiveProcs: IProc[] = [];
        const newActiveSystemProcs: ISystemProc[] = [];
        restoreState(db, newActiveTopics, systemState, newActiveProcs, newActiveSystemProcs, true, function(err) {
            if (err) return done.fail((err && err.message) || "init_error");
            expect(systemState.active).toBeTrue();
            expect(newActiveTopics).not.toContainKey("my_topic_0");
            expect(newActiveTopics).not.toContainKey("my_topic_1");
            expect(newActiveProcs).toHaveLength(0);
            expect(newActiveSystemProcs).toHaveLength(0);
            done();
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
    }], callback);
}
