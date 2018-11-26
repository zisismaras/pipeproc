//tslint:disable
import "jest-extended";
//tslint:enable
import {pipeProcClient} from "../../lib/client";

describe("committing logs", function() {

    beforeEach(function(done) {
        (<Promise<string>>pipeProcClient.spawn({memory: true})).then(function() {
            done();
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

    it("should commit a simple log and return a logId with sequenceNumber 0 and a correct timestamp", function(done) {
        (<Promise<string>>pipeProcClient.commit({
            topic: "my_topic",
            body: {
                hello: 1
            }
        })).then(function(logId) {
            expect(logId).toBeString();
            expect(logId.split("-")[1]).toEqual("0");
            expect(parseInt(logId.split("-")[0])).toBeLessThan(Date.now());
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have the sequence number increasing incrementally when multiple logs are committed", function(done) {
        (<Promise<string[]>>pipeProcClient.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }])).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            expect(logIds[0].split("-")[1]).toEqual("0");
            expect(logIds[1].split("-")[1]).toEqual("1");
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have the same and correct timestamps when multiple logs are committed", function(done) {
        (<Promise<string[]>>pipeProcClient.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }])).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            const ts1 = parseInt(logIds[0].split("-")[0]);
            const ts2 = parseInt(logIds[1].split("-")[0]);
            expect(ts1).toEqual(ts2);
            expect(ts1).toBeLessThan(Date.now());
            expect(ts2).toBeLessThan(Date.now());
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have sequenceNumbers begin at zero if the 2 logs are committed to different topics", function(done) {
        (<Promise<string[]>>pipeProcClient.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic_2",
            body: {
                hello: 2
            }
        }])).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            expect(logIds[0].split("-")[1]).toEqual("0");
            expect(logIds[1].split("-")[1]).toEqual("0");
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should return an error if the topic name is invalid", function(done) {
        (<Promise<string>>pipeProcClient.commit({
            topic: "an invalid topic",
            body: {
                hello: 1
            }
        })).then(function(logId) {
            done.fail("it should not accept invalid topic names");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_topic_format");
            done();
        });
    });

    it("should return an error if the log body is not an object", function(done) {
        //@ts-ignore
        (<Promise<string>>pipeProcClient.commit({
            topic: "my_topic",
            body: 123
        })).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });

    it("should return an error if the log body is missing", function(done) {
        //@ts-ignore
        (<Promise<string>>pipeProcClient.commit({
            topic: "my_topic"
        })).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });

    it("should return an error if the log body is null", function(done) {
        //@ts-ignore
        (<Promise<string>>pipeProcClient.commit({
            topic: "my_topic",
            body: null
        })).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });
});
