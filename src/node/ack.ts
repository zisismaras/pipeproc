import debug from "debug";
import LevelDOWN from "leveldown";
import {IProc} from "./proc";
import {transaction} from "./transaction";

const d = debug("pipeproc:node");

export function ack(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    procName: string,
    callback: (err?: Error|null, ackedLogId?: string) => void
): void {
    d("ack for proc:", procName);
    const myProc = activeProcs.find(p => p.name === procName);
    if (!myProc) {
        return callback(new Error("invalid_proc"));
    }
    d("acking:", myProc.lastClaimedRange);
    prepareAck(db, myProc).commitUpdate(function(err, lastAckedAt) {
        if (err) {
            callback(err);
        } else if (lastAckedAt) {
            myProc.lastAckedRange = myProc.lastClaimedRange;
            myProc.lastAckedAt = <number>lastAckedAt;
            callback(null, myProc.lastAckedRange);
        } else {
            callback(new Error("ack_failed"));
        }
    });
}

export function prepareAck(
    db: LevelDOWN.LevelDown,
    myProc: IProc
) {
    const prefix = `~~system~~#proc#${myProc.topic}#${myProc.name}#`;
    const ackedTimestamp = Date.now();
    const tx = transaction<number>(db);
    tx.add([{
        key: `${prefix}lastAckedRange`,
        value: `${myProc.lastClaimedRange}`
    }, {
        key: `${prefix}lastAckedAt`,
        value: `${ackedTimestamp}`
    }, {
        key: `${prefix}reclaims`,
        value: "0"
    }]);
    tx.done(function() {
        return ackedTimestamp;
    }, "ackTimestamps");

    return tx;
}
