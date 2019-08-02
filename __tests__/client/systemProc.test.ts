//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";

let client: IPipeProcClient;

beforeEach(async function() {
    client = PipeProc();

    await client.spawn({memory: true, workers: 1, namespace: uuid()});
});

afterEach(function() {
    return client.shutdown();
});

describe("systemProc smoke test", function() {
    it("should work", async function(done) {
        await client.systemProc({
            name: "my_system_proc",
            from: "topic_1",
            to: "topic_2",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable",
            processor: function(log, processorDone) {
                //@ts-ignore
                processorDone(null, {updatedNumber: log.body.number + 1});
            }
        });
        await client.commit({
            topic: "topic_1",
            body: {
                number: 1
            }
        });
        const lp = client.liveProc({
            topic: "topic_2",
            mode: "all"
        });
        lp.changes(async function(_err, result) {
            //@ts-ignore
            expect(result.body.updatedNumber).toBe(2);
            await lp.cancel();
            done();
        });
    });
});
