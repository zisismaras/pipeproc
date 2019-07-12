//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";
import {getRandomPort} from "../utils/getRandomPort";

//since revrange is just some sugar on range with {reverse: true} we only test some start/end cases

describe("using revrange", function() {
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
        await client.shutdown();
    });

    it("should not return results if start < end", async function() {
        const logs = await client.revrange("my_topic", {
            start: testLogIds[0],
            end: testLogIds[1]
        });
        expect(logs).toBeArrayOfSize(0);
    });

    it("should return the results in a reversed order", async function() {
        const logs = await client.revrange("my_topic", {
            start: testLogIds[2]
        });
        expect(logs).toBeArrayOfSize(3);
        expect(logs[0].id).toEqual(testLogIds[2]);
        expect(logs[1].id).toEqual(testLogIds[1]);
        expect(logs[2].id).toEqual(testLogIds[0]);
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
