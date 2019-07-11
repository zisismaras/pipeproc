//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";
import {getRandomPort} from "../utils/getRandomPort";

describe("committing logs", function() {
    let client: IPipeProcClient;

    beforeEach(function(done) {
        client = PipeProc();

        client.spawn({memory: true, workers: 0, namespace: uuid()}).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should commit a simple log and return a logId with sequenceNumber 0 and a correct timestamp", function(done) {
        client.commit({
            topic: "my_topic",
            body: {
                hello: 1
            }
        }).then(function(logId) {
            expect(logId).toBeString();
            expect((<string>logId).split("-")[1]).toEqual("0");
            expect(parseInt((<string>logId).split("-")[0])).toBeLessThanOrEqual(Date.now());
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have the sequence number increasing incrementally when multiple logs are committed", function(done) {
        client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }]).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            expect(logIds[0].split("-")[1]).toEqual("0");
            expect(logIds[1].split("-")[1]).toEqual("1");
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have the same and correct timestamps when multiple logs are committed", function(done) {
        client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }]).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            const ts1 = parseInt(logIds[0].split("-")[0]);
            const ts2 = parseInt(logIds[1].split("-")[0]);
            expect(ts1).toEqual(ts2);
            expect(ts1).toBeLessThanOrEqual(Date.now());
            expect(ts2).toBeLessThanOrEqual(Date.now());
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have sequenceNumbers begin at zero if the 2 logs are committed to different topics", function(done) {
        client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic_2",
            body: {
                hello: 2
            }
        }]).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            expect(logIds[0].split("-")[1]).toEqual("0");
            expect(logIds[1].split("-")[1]).toEqual("0");
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should return an error if the topic name is invalid", function(done) {
        client.commit({
            topic: "an invalid topic",
            body: {
                hello: 1
            }
        }).then(function(logId) {
            done.fail("it should not accept invalid topic names");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_topic_format");
            done();
        });
    });

    it("should return an error if the log body is not an object", function(done) {
        //@ts-ignore
        client.commit({
            topic: "my_topic",
            body: 123
        }).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });

    it("should return an error if the log body is missing", function(done) {
        //@ts-ignore
        client.commit({
            topic: "my_topic"
        }).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });

    it("should return an error if the log body is null", function(done) {
        //@ts-ignore
        client.commit({
            topic: "my_topic",
            body: null
        }).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });
});

describe("committing logs with TCP", function() {
    let client: IPipeProcClient;

    beforeEach(async function(done) {
        client = PipeProc();

        client.spawn({
            memory: true,
            workers: 0,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        }).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should commit a simple log and return a logId with sequenceNumber 0 and a correct timestamp", function(done) {
        client.commit({
            topic: "my_topic",
            body: {
                hello: 1
            }
        }).then(function(logId) {
            expect(logId).toBeString();
            expect((<string>logId).split("-")[1]).toEqual("0");
            expect(parseInt((<string>logId).split("-")[0])).toBeLessThanOrEqual(Date.now());
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have the sequence number increasing incrementally when multiple logs are committed", function(done) {
        client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }]).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            expect(logIds[0].split("-")[1]).toEqual("0");
            expect(logIds[1].split("-")[1]).toEqual("1");
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have the same and correct timestamps when multiple logs are committed", function(done) {
        client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }]).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            const ts1 = parseInt(logIds[0].split("-")[0]);
            const ts2 = parseInt(logIds[1].split("-")[0]);
            expect(ts1).toEqual(ts2);
            expect(ts1).toBeLessThanOrEqual(Date.now());
            expect(ts2).toBeLessThanOrEqual(Date.now());
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should have sequenceNumbers begin at zero if the 2 logs are committed to different topics", function(done) {
        client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic_2",
            body: {
                hello: 2
            }
        }]).then(function(logIds) {
            expect(logIds).toBeArrayOfSize(2);
            expect(logIds[0].split("-")[1]).toEqual("0");
            expect(logIds[1].split("-")[1]).toEqual("0");
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should return an error if the topic name is invalid", function(done) {
        client.commit({
            topic: "an invalid topic",
            body: {
                hello: 1
            }
        }).then(function(logId) {
            done.fail("it should not accept invalid topic names");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_topic_format");
            done();
        });
    });

    it("should return an error if the log body is not an object", function(done) {
        //@ts-ignore
        client.commit({
            topic: "my_topic",
            body: 123
        }).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });

    it("should return an error if the log body is missing", function(done) {
        //@ts-ignore
        client.commit({
            topic: "my_topic"
        }).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });

    it("should return an error if the log body is null", function(done) {
        //@ts-ignore
        client.commit({
            topic: "my_topic",
            body: null
        }).then(function(logId) {
            done.fail("it should not accept invalid log bodies");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_log_format");
            done();
        });
    });
});
