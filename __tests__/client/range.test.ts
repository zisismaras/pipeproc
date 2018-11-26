//tslint:disable
import "jest-extended";
//tslint:enable
import {pipeProcClient} from "../../lib/client";

describe("using range", function() {
    let testLogIds: string[];

    beforeEach(function(done) {
        (<Promise<string>>pipeProcClient.spawn({memory: true})).then(function() {
            commitSomeLogs().then(function(logs) {
                testLogIds = logs;
                done();
            }).catch(function(err) {
                done.fail(err);
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function(done) {
        if (pipeProcClient.pipeProcNode) {
            pipeProcClient.shutdown(function(err) {
                if (err) return done.fail(err);
                done();
            });
        } else {
            done();
        }
    });

    it("should get the whole topic if no start and end are provided", function(done) {
        pipeProcClient.range("my_topic", {}, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(3);
            done();
        });
    });

    it("should honor the limit option", function(done) {
        pipeProcClient.range("my_topic", {limit: 1}, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(1);
            done();
        });
    });

    it("should return the whole topic if limit is -1", function(done) {
        pipeProcClient.range("my_topic", {limit: -1}, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(3);
            done();
        });
    });

    it("should honor the start option", function(done) {
        pipeProcClient.range("my_topic", {start: testLogIds[1]}, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(2);
            expect(logs[0].id).toEqual(testLogIds[1]);
            done();
        });
    });

    it("should honor the end option", function(done) {
        pipeProcClient.range("my_topic", {end: testLogIds[1]}, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(2);
            expect(logs[1].id).toEqual(testLogIds[1]);
            done();
        });
    });

    it("should honor the start and end option", function(done) {
        pipeProcClient.range("my_topic", {start: testLogIds[1], end: testLogIds[2]}, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(2);
            expect(logs[0].id).toEqual(testLogIds[1]);
            expect(logs[1].id).toEqual(testLogIds[2]);
            done();
        });
    });

    it("should honor the exclusive option", function(done) {
        pipeProcClient.range("my_topic", {
            start: testLogIds[0],
            end: testLogIds[2],
            exclusive: true
        }, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(1);
            expect(logs[0].id).toEqual(testLogIds[1]);
            done();
        });
    });

    it("should accept a timestamp only", function(done) {
        pipeProcClient.range("my_topic", {
            start: testLogIds[0].split("-")[0]
        }, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(3);
            done();
        });
    });

    it("should accept a sequenceNumber only", function(done) {
        pipeProcClient.range("my_topic", {
            start: `:${testLogIds[2].split("-")[1]}`
        }, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(1);
            done();
        });
    });

    it("should return an error if the sequenceNumber(tone id) does not exist", function(done) {
        pipeProcClient.range("my_topic", {
            start: ":123"
        }, function(err, _logs) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_tone_id_search");
            done();
        });
    });

    it("should return an error if the start option is invalid", function(done) {
        pipeProcClient.range("my_topic", {
            start: "invalid"
        }, function(err, _logs) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_range_offset");
            done();
        });
    });

    it("should return an error if the end option is invalid", function(done) {
        pipeProcClient.range("my_topic", {
            end: "invalid"
        }, function(err, _logs) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_range_offset");
            done();
        });
    });

    it("should not return results if start > end", function(done) {
        pipeProcClient.range("my_topic", {
            start: testLogIds[1],
            end: testLogIds[0]
        }, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(0);
            done();
        });
    });

});

function commitSomeLogs() {
    return (<Promise<string[]>>pipeProcClient.commit([{
        topic: "my_topic",
        body: {
            hello: 1
        }
    }, {
        topic: "my_topic",
        body: {
            hello: 1
        }
    }, {
        topic: "my_topic",
        body: {
            hello: 1
        }
    }]));
}
