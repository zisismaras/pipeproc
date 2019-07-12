//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {getRandomPort} from "../utils/getRandomPort";

describe("committing logs with TCP", function() {
    let client: IPipeProcClient;

    beforeEach(async function() {
        client = PipeProc();

        return client.spawn({
            memory: true,
            workers: 0,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        });
    });

    afterEach(async function() {
        await client.shutdown();
    });

    it("should commit a simple log and return a logId with sequenceNumber 0 and a correct timestamp", async function() {
        const logId = await client.commit({
            topic: "my_topic",
            body: {
                hello: 1
            }
        });
        expect(logId).toBeString();
        expect((<string>logId).split("-")[1]).toEqual("0");
        expect(parseInt((<string>logId).split("-")[0])).toBeLessThanOrEqual(Date.now());
    });

    it("should have the sequence number increasing incrementally when multiple logs are committed", async function() {
        const logIds = await client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }]);
        expect(logIds).toBeArrayOfSize(2);
        expect(logIds[0].split("-")[1]).toEqual("0");
        expect(logIds[1].split("-")[1]).toEqual("1");
    });

    it("should have the same and correct timestamps when multiple logs are committed", async function() {
        const logIds = await client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic",
            body: {
                hello: 2
            }
        }]);
        expect(logIds).toBeArrayOfSize(2);
        const ts1 = parseInt(logIds[0].split("-")[0]);
        const ts2 = parseInt(logIds[1].split("-")[0]);
        expect(ts1).toEqual(ts2);
        expect(ts1).toBeLessThanOrEqual(Date.now());
        expect(ts2).toBeLessThanOrEqual(Date.now());
    });

    it("should have sequenceNumbers begin at zero if the 2 logs are committed to different topics", async function() {
        const logIds = await client.commit([{
            topic: "my_topic",
            body: {
                hello: 1
            }
        }, {
            topic: "my_topic_2",
            body: {
                hello: 2
            }
        }]);
        expect(logIds).toBeArrayOfSize(2);
        expect(logIds[0].split("-")[1]).toEqual("0");
        expect(logIds[1].split("-")[1]).toEqual("0");
    });

    it("should return an error if the topic name is invalid", async function() {
        expect((async function() {
            await client.commit({
                topic: "an invalid topic",
                body: {
                    hello: 1
                }
            });
        })()).rejects.toThrowError("invalid_topic_format");
    });

    it("should return an error if the log body is not an object", async function() {
        expect((async function() {
            //@ts-ignore
            await client.commit({
                topic: "my_topic",
                body: 123
            });
        })()).rejects.toThrowError("invalid_log_format");
    });

    it("should return an error if the log body is missing", async function() {
        expect((async function() {
            //@ts-ignore
            await client.commit({
                topic: "my_topic"
            });
        })()).rejects.toThrowError("invalid_log_format");
    });

    it("should return an error if the log body is null", async function() {
        expect((async function() {
            //@ts-ignore
            await client.commit({
                topic: "my_topic",
                body: null
            });
        })()).rejects.toThrowError("invalid_log_format");
    });
});
