//tslint:disable
import "jest-extended";
//tslint:enable
import {LevelDown as LevelDOWN} from "leveldown";
import MemDOWN from "memdown";
import {commitLog} from "../../src/node/commitLog";
import {proc as _proc, IProc, getAvailableProc as _getAvailableProc} from "../../src/node/proc";
import {ack as _ack} from "../../src/node/ack";
import {disableProc as _disableProc} from "../../src/node/resumeDisableProc";
import {IActiveTopics} from "../../src/node/pipeProc";
import {promisify} from "util";
import {IRangeResult} from "../../src/node/getRange";

const proc = promisify(_proc);
const ack = promisify(_ack);
const getAvailableProc = promisify(_getAvailableProc);
const disableProc = promisify(_disableProc);

describe("availableProc", function() {
    let db: LevelDOWN;
    let activeTopics: IActiveTopics;
    let activeProcs: IProc[];
    let addedIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        activeProcs = [];
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
                done.fail(err);
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    done();
                } else {
                    done.fail("Invalid logs created");
                }
            }
        });
    });

    it("should first return a proc that hasn't been created yet", async function() {
        //create the first proc
        await proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await ack(db, activeProcs, "my_proc_0");
        //it should return the new proc my_proc_1
        let result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        expect(result.procName).toBe("my_proc_1");
        expect((<IRangeResult>result.log).id).toBe(addedIds[0]);
        //it should return the new proc my_proc_2
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_2",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        expect(result.procName).toBe("my_proc_2");
        expect((<IRangeResult>result.log).id).toBe(addedIds[0]);
    });

    it("should return nothing if there is no proc with work", async function() {
        //my_proc_0 has acked 3/3 logs of its topic
        await proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await ack(db, activeProcs, "my_proc_0");
        await proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await ack(db, activeProcs, "my_proc_0");
        await proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await ack(db, activeProcs, "my_proc_0");
        const result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        expect(result).toBeUndefined();
    });

    it("round robin works", async function() {
        //the first proc is created and returned
        let result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_0");
        //the second proc is created and returned
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_1");
        //round robin should select the first proc
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_0");
        //round robin should select the second proc
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_1");
        //round robin should select the first proc
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_0");
        //round robin should select the second proc
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_1");
    });

    it("should not use procs that are disabled", async function() {
        //create the first proc
        await proc(db, activeProcs, activeTopics, {
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await ack(db, activeProcs, "my_proc_0");
        //create the second proc
        await proc(db, activeProcs, activeTopics, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        });
        await ack(db, activeProcs, "my_proc_1");
        //round robin should select the first proc
        let result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_0");
        //disable my_proc_1
        await disableProc(db, activeProcs, "my_proc_1");
        //round robin should select the first proc again since my_proc_1 is disabled
        result = await getAvailableProc(db, activeProcs, activeTopics, [{
            name: "my_proc_0",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }, {
            name: "my_proc_1",
            topic: "my_topic_0",
            offset: ">",
            count: 1,
            maxReclaims: 10,
            reclaimTimeout: 10000,
            onMaxReclaimsReached: "disable"
        }]);
        await ack(db, activeProcs, result.procName);
        expect(result.procName).toBe("my_proc_0");
    });
});

function addLogsToTopic(
    db: LevelDOWN,
    activeTopics: IActiveTopics,
    callback: (err: Error|null, ids?: string|string[]) => void)
: void {
    commitLog(db, activeTopics, [{
        topic: "my_topic_0",
        body: "{\"myData\": 1}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 2}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 3}"
    }], function(err, ids) {
        setTimeout(function() {
            callback(err, ids);
        }, 20);
    });
}
