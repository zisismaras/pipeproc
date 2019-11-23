//tslint:disable
import "jest-extended";
//tslint:enable
import {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {parallel} from "async";
import {commitLog} from "../../src/node/commitLog";
import {IActiveTopics} from "../../src/node/pipeProc";
import {FIRST_TONE, SECOND_TONE} from "../../src/node/tones";

const LOG_ID_FORMAT = /^([0-9]{13}-[0-9]+)$/;

describe("add a single log", function() {
    test("the log should have a valid format", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, {
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, function(err, id) {
            if (err) return done.fail(err);
            expect(id).toMatch(LOG_ID_FORMAT);
            done();
        });
    });
    test("the first log should have a tone number of 1(zero-padded)", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, {
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, function(err, id) {
            if (err) return done.fail(err);
            expect((<string>id).split("-")[1]).toBe(FIRST_TONE);
            done();
        });
    });
    test("the second log should have a tone number of 2(zero-padded)", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, {
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, function() {
            commitLog(db, activeTopics, {
                topic: "my_topic",
                body: "{\"myData\": 2}"
            }, function(err, id) {
                if (err) return done.fail(err);
                expect((<string>id).split("-")[1]).toBe(SECOND_TONE);
                done();
            });
        });
    });
    test("it should return an error if there was a problem with leveldb batch", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        //@ts-ignore
        db.batch = jest.fn(function(err, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        commitLog(db, activeTopics, {
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, function(err, id) {
            expect(err).toBeInstanceOf(Error);
            expect(id).toBeUndefined();
            done();
        });
    });
    test("it should return an error if there was a problem with leveldb put", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        //@ts-ignore
        db.batch = jest.fn(function(err, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        //@ts-ignore
        db.put = jest.fn(function(_k, _v, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        commitLog(db, activeTopics, {
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, function(err, id) {
            expect(err).toBeInstanceOf(Error);
            expect(id).toBeUndefined();
            done();
        });
    });
});

describe("add multiple logs to the same topic", function() {
    test("it should return an array of ids", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            expect(ids).toBeArray();
            expect(ids).toHaveLength(3);
            done();
        });
    });
    test("they should have a valid format", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                ids.forEach(function(id) {
                    expect(id).toMatch(LOG_ID_FORMAT);
                });
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
    test("the ids should have a correct incrementing tone number", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                ids.forEach(function(id, i) {
                    expect(id).toEndWith(`${i + 1}`);
                });
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
    test("it should return an error if there was a problem with leveldb batch", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        //@ts-ignore
        db.batch = jest.fn(function(err, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        commitLog(db, activeTopics, [{
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            expect(err).toBeInstanceOf(Error);
            expect(ids).toBeUndefined();
            done();
        });
    });
    test("it should return an error if there was a problem with leveldb put", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        //@ts-ignore
        db.batch = jest.fn(function(err, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        //@ts-ignore
        db.put = jest.fn(function(_k, _v, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        commitLog(db, activeTopics, [{
            topic: "my_topic",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            expect(err).toBeInstanceOf(Error);
            expect(ids).toBeUndefined();
            done();
        });
    });
    test("toneId should increment correctly even if adding multiple logs at the same time", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        parallel([
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            },
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            },
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            },
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            }
        ], function(err: Error, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                expect(ids[0]).toEndWith("01");
                expect(ids[1]).toEndWith("02");
                expect(ids[2]).toEndWith("03");
                expect(ids[3]).toEndWith("04");
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
});

describe("add multiple logs to different topics", function() {
    test("it should return an array of ids", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic_0",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_2",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            expect(ids).toBeArray();
            expect(ids).toHaveLength(3);
            done();
        });
    });
    test("they should have a valid format", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic_0",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_2",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                ids.forEach(function(id) {
                    expect(id).toMatch(LOG_ID_FORMAT);
                });
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
    test("the logs should start with a tone number of 1(zero-padded)", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic_0",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_2",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                ids.forEach(function(id) {
                    expect(id).toEndWith("01");
                });
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
    test("the logs should retain their correct tone number", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        commitLog(db, activeTopics, [{
            topic: "my_topic_0",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_0",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 3}"
        }, {
            topic: "my_topic_0",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                expect(ids[0]).toEndWith("01");
                expect(ids[1]).toEndWith("01");
                expect(ids[2]).toEndWith("02");
                expect(ids[3]).toEndWith("02");
                expect(ids[4]).toEndWith("03");
                expect(ids[5]).toEndWith("03");
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
    test("it should return an error if there was a problem with leveldb batch", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        //@ts-ignore
        db.batch = jest.fn(function(err, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        commitLog(db, activeTopics, [{
            topic: "my_topic_0",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_2",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            expect(err).toBeInstanceOf(Error);
            expect(ids).toBeUndefined();
            done();
        });
    });
    test("it should return an error if there was a problem with leveldb put", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        //@ts-ignore
        db.batch = jest.fn(function(err, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        //@ts-ignore
        db.put = jest.fn(function(_k, _v, callback) {
            //@ts-ignore
            callback(new Error("Commit Error"));
        });
        commitLog(db, activeTopics, [{
            topic: "my_topic_0",
            body: "{\"myData\": 1}"
        }, {
            topic: "my_topic_1",
            body: "{\"myData\": 2}"
        }, {
            topic: "my_topic_2",
            body: "{\"myData\": 3}"
        }], function(err, ids) {
            expect(err).toBeInstanceOf(Error);
            expect(ids).toBeUndefined();
            done();
        });
    });
    test("toneId should increment correctly even if adding multiple logs at the same time", function(done) {
        const db: LevelDOWN = MemDOWN();
        const activeTopics: IActiveTopics = {};
        parallel([
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            },
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_1",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            },
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_0",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            },
            function(cb) {
                commitLog(db, activeTopics, {
                    topic: "my_topic_1",
                    body: "{\"myData\": 1}"
                }, function(err, id) {
                    cb(err, id);
                });
            }
        ], function(err: {message: string}, ids) {
            if (err) return done.fail(err);
            if (ids && Array.isArray(ids)) {
                expect(ids[0]).toEndWith("01");
                expect(ids[1]).toEndWith("01");
                expect(ids[2]).toEndWith("02");
                expect(ids[3]).toEndWith("02");
                done();
            } else {
                done.fail("ids was not an array");
            }
        });
    });
});
