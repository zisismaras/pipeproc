//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";
import {getRandomPort} from "../utils/getRandomPort";

if (process.platform !== "win32") {
    describe("using range", function() {
        let testLogIds: string[];
        let client: IPipeProcClient;

        beforeEach(function(done) {
            client = PipeProc();

            client.spawn({memory: true, workers: 0, namespace: uuid()}).then(function() {
                commitSomeLogs(client).then(function(logs) {
                    testLogIds = logs;
                    done();
                }).catch(function(err) {
                    done.fail(err);
                });
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

        it("should get the whole topic if no start and end are provided", function(done) {
            client.range("my_topic").then(function(logs) {
                expect(logs).toBeArrayOfSize(3);
                done();
            });
        });

        it("should honor the limit option", function(done) {
            client.range("my_topic", {limit: 1}).then(function(logs) {
                expect(logs).toBeArrayOfSize(1);
                done();
            });
        });

        it("should return the whole topic if limit is -1", function(done) {
            client.range("my_topic", {limit: -1}).then(function(logs) {
                expect(logs).toBeArrayOfSize(3);
                done();
            });
        });

        it("should honor the start option", function(done) {
            client.range("my_topic", {start: testLogIds[1]}).then(function(logs) {
                expect(logs).toBeArrayOfSize(2);
                expect(logs[0].id).toEqual(testLogIds[1]);
                done();
            });
        });

        it("should honor the end option", function(done) {
            client.range("my_topic", {end: testLogIds[1]}).then(function(logs) {
                expect(logs).toBeArrayOfSize(2);
                expect(logs[1].id).toEqual(testLogIds[1]);
                done();
            });
        });

        it("should honor the start and end option", function(done) {
            client.range("my_topic", {start: testLogIds[1], end: testLogIds[2]}).then(function(logs) {
                expect(logs).toBeArrayOfSize(2);
                expect(logs[0].id).toEqual(testLogIds[1]);
                expect(logs[1].id).toEqual(testLogIds[2]);
                done();
            });
        });

        it("should honor the exclusive option", function(done) {
            client.range("my_topic", {
                start: testLogIds[0],
                end: testLogIds[2],
                exclusive: true
            }).then(function(logs) {
                expect(logs).toBeArrayOfSize(1);
                expect(logs[0].id).toEqual(testLogIds[1]);
                done();
            });
        });

        it("should accept a timestamp only", function(done) {
            client.range("my_topic", {
                start: testLogIds[0].split("-")[0]
            }).then(function(logs) {
                expect(logs).toBeArrayOfSize(3);
                done();
            });
        });

        it("should accept a sequenceNumber only", function(done) {
            client.range("my_topic", {
                start: `:${testLogIds[2].split("-")[1]}`
            }).then(function(logs) {
                expect(logs).toBeArrayOfSize(1);
                done();
            });
        });

        it("should return an error if the sequenceNumber(tone id) does not exist", function(done) {
            client.range("my_topic", {
                start: ":123"
            }).catch(function(err) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toEqual("invalid_tone_id_search");
                done();
            });
        });

        it("should return an error if the start option is invalid", function(done) {
            client.range("my_topic", {
                start: "invalid"
            }).catch(function(err) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toEqual("invalid_range_offset");
                done();
            });
        });

        it("should return an error if the end option is invalid", function(done) {
            client.range("my_topic", {
                end: "invalid"
            }).catch(function(err) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toEqual("invalid_range_offset");
                done();
            });
        });

        it("should not return results if start > end", function(done) {
            client.range("my_topic", {
                start: testLogIds[1],
                end: testLogIds[0]
            }).then(function(logs) {
                expect(logs).toBeArrayOfSize(0);
                done();
            });
        });

    });
}

describe("using range with TCP", function() {
    let testLogIds: string[];
    let client: IPipeProcClient;

    beforeEach(async function() {
        client = PipeProc();

        await client.spawn({
            memory: true,
            workers: 0,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        });
        testLogIds = await commitSomeLogs(client);
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should get the whole topic if no start and end are provided", function(done) {
        client.range("my_topic").then(function(logs) {
            expect(logs).toBeArrayOfSize(3);
            done();
        });
    });

    it("should honor the limit option", function(done) {
        client.range("my_topic", {limit: 1}).then(function(logs) {
            expect(logs).toBeArrayOfSize(1);
            done();
        });
    });

    it("should return the whole topic if limit is -1", function(done) {
        client.range("my_topic", {limit: -1}).then(function(logs) {
            expect(logs).toBeArrayOfSize(3);
            done();
        });
    });

    it("should honor the start option", function(done) {
        client.range("my_topic", {start: testLogIds[1]}).then(function(logs) {
            expect(logs).toBeArrayOfSize(2);
            expect(logs[0].id).toEqual(testLogIds[1]);
            done();
        });
    });

    it("should honor the end option", function(done) {
        client.range("my_topic", {end: testLogIds[1]}).then(function(logs) {
            expect(logs).toBeArrayOfSize(2);
            expect(logs[1].id).toEqual(testLogIds[1]);
            done();
        });
    });

    it("should honor the start and end option", function(done) {
        client.range("my_topic", {start: testLogIds[1], end: testLogIds[2]}).then(function(logs) {
            expect(logs).toBeArrayOfSize(2);
            expect(logs[0].id).toEqual(testLogIds[1]);
            expect(logs[1].id).toEqual(testLogIds[2]);
            done();
        });
    });

    it("should honor the exclusive option", function(done) {
        client.range("my_topic", {
            start: testLogIds[0],
            end: testLogIds[2],
            exclusive: true
        }).then(function(logs) {
            expect(logs).toBeArrayOfSize(1);
            expect(logs[0].id).toEqual(testLogIds[1]);
            done();
        });
    });

    it("should accept a timestamp only", function(done) {
        client.range("my_topic", {
            start: testLogIds[0].split("-")[0]
        }).then(function(logs) {
            expect(logs).toBeArrayOfSize(3);
            done();
        });
    });

    it("should accept a sequenceNumber only", function(done) {
        client.range("my_topic", {
            start: `:${testLogIds[2].split("-")[1]}`
        }).then(function(logs) {
            expect(logs).toBeArrayOfSize(1);
            done();
        });
    });

    it("should return an error if the sequenceNumber(tone id) does not exist", function(done) {
        client.range("my_topic", {
            start: ":123"
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_tone_id_search");
            done();
        });
    });

    it("should return an error if the start option is invalid", function(done) {
        client.range("my_topic", {
            start: "invalid"
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_range_offset");
            done();
        });
    });

    it("should return an error if the end option is invalid", function(done) {
        client.range("my_topic", {
            end: "invalid"
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("invalid_range_offset");
            done();
        });
    });

    it("should not return results if start > end", function(done) {
        client.range("my_topic", {
            start: testLogIds[1],
            end: testLogIds[0]
        }).then(function(logs) {
            expect(logs).toBeArrayOfSize(0);
            done();
        });
    });

});

function commitSomeLogs(client) {
    return client.commit([{
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
    }]);
}
