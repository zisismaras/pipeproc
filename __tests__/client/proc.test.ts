//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";

let testLogIds: string | string[];
let client: IPipeProcClient;

beforeEach(async function() {
    client = PipeProc();

    await client.spawn({memory: true, workers: 0, namespace: uuid()});
    testLogIds = await commitSomeLogs(client);
});

afterEach(function() {
    return client.shutdown();
});

describe("proc tests", function() {
    it("should return the very first log on the first run", async function() {
        const log = await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        expect(log).toBeDefined();
        if (!Array.isArray(log)) {
            expect(log.id).toBe(testLogIds[0]);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should be able to return multiple logs at a time", async function() {
        const logs = await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 3,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        expect(logs).toBeDefined();
        expect(logs).toBeArray();
        expect(logs).toHaveLength(3);
        if (Array.isArray(logs)) {
            expect(logs[0].id).toBe(testLogIds[0]);
            expect(logs[1].id).toBe(testLogIds[1]);
            expect(logs[2].id).toBe(testLogIds[2]);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should not return a log if it is called twice without ack", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const log = await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        expect(log).toBeUndefined();
    });

    it("should return the second log after acking the first one", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await client.ack("my_proc_0");
        const log = await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        expect(log).toBeDefined();
        if (!Array.isArray(log)) {
            expect(log.id).toBe(testLogIds[1]);
            const myProc = await client.inspectProc("my_proc_0");
            expect(myProc.lastAckedRange).toBe(`${testLogIds[0]}..${testLogIds[0]}`);
            expect(myProc.lastClaimedRange).toBe(`${testLogIds[1]}..${testLogIds[1]}`);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should return the second log after ackComitting the first one", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const ackResult = await client.ackCommit("my_proc_0", {
            topic: "my_topic_0",
            body: {someData: 1}
        });
        expect(ackResult.ackedLogId).toBe(`${testLogIds[0]}..${testLogIds[0]}`);
        const log = await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        expect(log).toBeDefined();
        if (!Array.isArray(log)) {
            expect(log.id).toBe(testLogIds[1]);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should return the first log again after calling reclaim", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const reclaimId = await client.reclaimProc("my_proc_0");
        expect(reclaimId).toBe("");
        const log = await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        expect(log).toBeDefined();
        if (!Array.isArray(log)) {
            expect(log.id).toBe(testLogIds[0]);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should return a correct reclaimId", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await client.ack("my_proc_0");
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const reclaimId = await client.reclaimProc("my_proc_0");
        expect(reclaimId).toBe(`${testLogIds[0]}..${testLogIds[0]}`);
    });

    it("should be able to inspect a proc", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const proc = await client.inspectProc("my_proc_0");
        expect(proc.name).toBe("my_proc_0");
    });

    it("should return the very first log if availableProc is used", async function() {
        const {procName, log} = await client.availableProc([{
            topic: "my_topic_0",
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        expect(log).toBeDefined();
        expect(procName).toBe("my_proc_0");
        if (!Array.isArray(log)) {
            expect(log.id).toBe(testLogIds[0]);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should return all the logs if count = 3 and availableProc is used", async function() {
        const {procName, log} = await client.availableProc([{
            topic: "my_topic_0",
            name: "my_proc_0",
            offset: ">",
            count: 3,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        expect(procName).toBe("my_proc_0");
        expect(log).toBeDefined();
        expect(log).toBeArray();
        expect(log).toHaveLength(3);
        if (Array.isArray(log)) {
            expect(log[0].id).toBe(testLogIds[0]);
            expect(log[1].id).toBe(testLogIds[1]);
            expect(log[2].id).toBe(testLogIds[2]);
        } else {
            throw new Error("invalid_proc_result");
        }
    });

    it("should be able to disable and resume a proc", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const proc1 = await client.disableProc("my_proc_0");
        expect(proc1.status).toBe("disabled");
        const proc2 = await client.resumeProc("my_proc_0");
        expect(proc2.status).toBe("active");
    });

    it("should be able to destroy a proc", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await client.destroyProc("my_proc_0");
        expect((async function() {
            await client.inspectProc("my_proc_0");
        })()).rejects.toThrowError("invalid_proc");
    });

    it("waitForProcs should resolve when all the topics of the proc have been acked", async function() {
        await client.proc("my_topic_0", {
            name: "my_proc_0",
            offset: ">",
            count: 3,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        const result = await Promise.all([
            client.ack("my_proc_0"),
            client.waitForProcs()
        ]);
        expect(result[0]).toBe(`${testLogIds[0]}..${testLogIds[2]}`);
    });
});

function commitSomeLogs(cl: IPipeProcClient) {
    return cl.commit([{
        topic: "my_topic_0",
        body: {
            hello: 1
        }
    }, {
        topic: "my_topic_0",
        body: {
            hello: 1
        }
    }, {
        topic: "my_topic_0",
        body: {
            hello: 1
        }
    }]);
}
