//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {getRandomPort} from "../utils/getRandomPort";

describe("using range", function() {
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

    afterEach(async function() {
        try {
            await client.shutdown();
        } catch (_e) {}
    });

    it("should get the whole topic if no start and end are provided", async function() {
        const logs = await client.range("my_topic");
        expect(logs).toBeArrayOfSize(3);
    });

    it("should honor the limit option", async function() {
        const logs = await client.range("my_topic", {limit: 1});
        expect(logs).toBeArrayOfSize(1);
    });

    it("should return the whole topic if limit is -1", async function() {
        const logs = await client.range("my_topic", {limit: -1});
        expect(logs).toBeArrayOfSize(3);
    });

    it("should honor the start option", async function() {
        const logs = await client.range("my_topic", {start: testLogIds[1]});
        expect(logs).toBeArrayOfSize(2);
        expect(logs[0].id).toEqual(testLogIds[1]);
    });

    it("should honor the end option", async function() {
        const logs = await client.range("my_topic", {end: testLogIds[1]});
        expect(logs).toBeArrayOfSize(2);
        expect(logs[1].id).toEqual(testLogIds[1]);
    });

    it("should honor the start and end option", async function() {
        const logs = await client.range("my_topic", {start: testLogIds[1], end: testLogIds[2]});
        expect(logs).toBeArrayOfSize(2);
        expect(logs[0].id).toEqual(testLogIds[1]);
        expect(logs[1].id).toEqual(testLogIds[2]);
    });

    it("should honor the exclusive option", async function() {
        const logs = await client.range("my_topic", {
            start: testLogIds[0],
            end: testLogIds[2],
            exclusive: true
        });
        expect(logs).toBeArrayOfSize(1);
        expect(logs[0].id).toEqual(testLogIds[1]);
    });

    it("should accept a timestamp only", async function() {
        const logs = await client.range("my_topic", {
            start: testLogIds[0].split("-")[0]
        });
        expect(logs).toBeArrayOfSize(3);
    });

    it("should accept a sequenceNumber only", async function() {
        const logs = await client.range("my_topic", {
            start: `:${testLogIds[2].split("-")[1]}`
        });
        expect(logs).toBeArrayOfSize(1);
    });

    it("should return an error if the sequenceNumber(tone id) does not exist", async function() {
        expect((async function() {
            await client.range("my_topic", {
                start: ":123"
            });
        })()).rejects.toThrowError("invalid_tone_id_search");
    });

    it("should return an error if the start option is invalid", async function() {
        expect((async function() {
            await client.range("my_topic", {
                start: "invalid"
            });
        })()).rejects.toThrowError("invalid_range_offset");
    });

    it("should return an error if the end option is invalid", async function() {
        expect((async function() {
            await client.range("my_topic", {
                end: "invalid"
            });
        })()).rejects.toThrowError("invalid_range_offset");
    });

    it("should not return results if start > end", async function() {
        const logs = await client.range("my_topic", {
            start: testLogIds[1],
            end: testLogIds[0]
        });
        expect(logs).toBeArrayOfSize(0);
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
