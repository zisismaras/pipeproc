//tslint:disable
import "jest-extended";
//tslint:enable
import LevelDOWN from "leveldown";
import MemDOWN from "memdown";
import {IProc, proc} from "../../src/node/proc";
import {IActiveTopics} from "../../src/node/pipeProc";
import {ISystemProc, validateProcs, systemProc} from "../../src/node/systemProc";
import {IWorker} from "../../src/node/workerManager";

describe("validate procs with a single 'from'", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    beforeEach(function() {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
    });
    it("should return undefined if the proc does not already exists", function(done) {
        validateProcs("my_proc", "my_topic", activeProcs, function(err, myProc) {
            if (err) return done.fail(err);
            expect(myProc).toBeUndefined();
            done();
        });
    });
    it("should return the proc if the proc already exists", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc",
            topic: "my_topic",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            validateProcs("my_proc", "my_topic", activeProcs, function(err, myProc) {
                if (err) return done.fail(err);
                expect(myProc).toBe(activeProcs[0]);
                done();
            });
        });
    });
    it("should return an error if the proc already exists and is not unique", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc",
            topic: "my_topic",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            validateProcs("my_proc", "another_topic", activeProcs, function(err, myProc) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toBe("proc_name_not_unique");
                expect(myProc).toBeUndefined();
                done();
            });
        });
    });
});

describe("validate procs with multiple 'from'", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    beforeEach(function() {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
    });
    it("should return an empty array if the procs do not already exists", function(done) {
        validateProcs("my_proc", ["my_topic", "my_topic_1"], activeProcs, function(err, myProcs) {
            if (err) return done.fail(err);
            expect(myProcs).toBeArray();
            expect(myProcs).toHaveLength(0);
            done();
        });
    });
    it("should return the procs that alredy exist", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc__my_topic",
            topic: "my_topic",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            validateProcs("my_proc", ["my_topic", "my_topic_1"], activeProcs, function(err, myProcs) {
                if (err) return done.fail(err);
                expect(myProcs).toBeArray();
                expect(myProcs).toHaveLength(1);
                done();
            });
        });
    });
    it("should return an error if some procs already exists and are not unique", function(done) {
        proc(db, activeProcs, activeTopics, {
            name: "my_proc__my_topic",
            topic: "another_topic",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(procErr) {
            if (procErr) return done.fail(procErr);
            validateProcs("my_proc", ["my_topic", "my_topic_1"], activeProcs, function(err, myProc) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toBe("proc_name_not_unique");
                expect(myProc).toBeUndefined();
                done();
            });
        });
    });
});

describe("create procs with a single 'from'", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let activeSystemProcs: ISystemProc[];
    let activeWorkers: IWorker[];
    beforeEach(function() {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        activeSystemProcs = [];
        activeWorkers = [];
    });
    it("should be created (inlineProcessor)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: "my_topic",
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(1);
            expect(myProc).toBe(activeProcs[0]);
            done();
        });
    });
    it("should be created (single to)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: "my_topic",
            to: "another_topic_1",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(1);
            expect(myProc).toBe(activeProcs[0]);
            done();
        });
    });
    it("should be created (multiple to's)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: "my_topic",
            to: ["another_topic_1", "another_topic_2"],
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(1);
            expect(myProc).toBe(activeProcs[0]);
            done();
        });
    });
    it("should be created (multiple from, multiple to)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: ["my_topic", "my_topic_1"],
            to: ["another_topic_1", "another_topic_2"],
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(2);
            expect(myProc[0]).toBe(activeProcs[0]);
            expect(myProc[1]).toBe(activeProcs[1]);
            done();
        });
    });
    it("should be created (externalProcessor)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: "my_topic",
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            externalProcessor: "./myScript.js"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(1);
            expect(myProc).toBe(activeProcs[0]);
            done();
        });
    });
    it("should return an error if no processor is provided", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: "my_topic",
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, function(err, myProc) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toBe("no_valid_processor_provided");
            done();
        });
    });
    it("should just return the proc if it already exists", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: "my_topic",
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            externalProcessor: "./myScript.js"
        }, function(err) {
            if (err) return done.fail(err);
            systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
                name: "my_proc",
                from: "my_topic",
                to: "",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable",
                externalProcessor: "./myScript.js"
            }, function(err2, myProc) {
                if (err2) return done.fail(err2);
                expect(activeSystemProcs).toHaveLength(1);
                expect(myProc).toBe(activeProcs[0]);
                done();
            });
        });
    });
});

describe("create procs with multiple 'from'", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let activeSystemProcs: ISystemProc[];
    let activeWorkers: IWorker[];
    beforeEach(function() {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        activeSystemProcs = [];
        activeWorkers = [];
    });
    it("should be created (inlineProcessor)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: ["my_topic", "my_topic_0"],
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(2);
            expect(activeProcs).toHaveLength(2);
            expect(myProc[0]).toBe(activeProcs[0]);
            expect(myProc[1]).toBe(activeProcs[1]);
            done();
        });
    });
    it("should be created (externalProcessor)", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: ["my_topic", "my_topic_0"],
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            externalProcessor: "./myScript.js"
        }, function(err, myProc) {
            if (err) return done.fail(err);
            expect(activeSystemProcs).toHaveLength(2);
            expect(activeProcs).toHaveLength(2);
            expect(myProc[0]).toBe(activeProcs[0]);
            expect(myProc[1]).toBe(activeProcs[1]);
            done();
        });
    });
    it("should just return the procs if they already exist", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc",
            from: ["my_topic", "my_topic_0"],
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err) {
            if (err) return done.fail(err);
            systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
                name: "my_proc",
                from: ["my_topic", "my_topic_0"],
                to: "",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable",
                inlineProcessor: "function(log, done) {done()}"
            }, function(err2, myProc) {
                if (err2) return done.fail(err2);
                expect(activeSystemProcs).toHaveLength(2);
                expect(activeProcs).toHaveLength(2);
                expect(myProc[0]).toBe(activeProcs[0]);
                expect(myProc[1]).toBe(activeProcs[1]);
                done();
            });
        });
    });
    it("should create only those that do not already exist", function(done) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
            name: "my_proc__my_topic",
            from: "my_topic",
            to: "",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            inlineProcessor: "function(log, done) {done()}"
        }, function(err) {
            if (err) return done.fail(err);
            systemProc(db, activeProcs, activeSystemProcs, activeWorkers, {
                name: "my_proc",
                from: ["my_topic", "my_topic_0"],
                to: "",
                offset: ">",
                count: 1,
                maxReclaims: 10,
                reclaimTimeout: 10000,
                onMaxReclaimsReached: "disable",
                inlineProcessor: "function(log, done) {done()}"
            }, function(err2, myProc) {
                if (err2) return done.fail(err2);
                expect(activeSystemProcs).toHaveLength(2);
                expect(activeProcs).toHaveLength(2);
                expect(myProc).toBe(activeProcs[1]);
                done();
            });
        });
    });
});
