import debug from "debug";
import {LevelDown as LevelDOWN} from "leveldown";
import {IProc} from "./proc";
import {transaction} from "./transaction";
const d = debug("pipeproc:node");

export function disableProc(
    db: LevelDOWN,
    activeProcs: IProc[],
    procName: string,
    callback: (err?: Error|null, proc?: IProc) => void
): void {
    const myProc = activeProcs.find(p => p.name === procName);
    if (!myProc) {
        return callback(new Error("invalid_proc"));
    }
    if (myProc.status === "disabled") {
        return callback(new Error("proc_already_disabled"));
    }
    d("disabling proc:", myProc.name);
    const prefix = `~~system~~#proc#${myProc.topic}#${myProc.name}#`;
    const tx = transaction(db);
    tx.add({
        key: `${prefix}status`,
        value: "disabled"
    });
    tx.commitUpdate(function(err) {
        if (err) {
            callback(err);
        } else {
            myProc.status = "disabled";
            callback(null, myProc);
        }
    });
}

export function resumeProc(
    db: LevelDOWN,
    activeProcs: IProc[],
    procName: string,
    callback: (err?: Error|null, proc?: IProc) => void
): void {
    const myProc = activeProcs.find(p => p.name === procName);
    if (!myProc) {
        return callback(new Error("invalid_proc"));
    }
    if (myProc.status === "active") {
        return callback(new Error("proc_already_active"));
    }
    d("resuming proc:", myProc.name);
    const tx = transaction(db);
    const prefix = `~~system~~#proc#${myProc.topic}#${myProc.name}#`;
    tx.add([{
        key: `${prefix}status`,
        value: "active"
    }, {
        key: `${prefix}reclaims`,
        value: "0"
    }]);
    tx.commitUpdate(function(err) {
        if (err) {
            callback(err);
        } else {
            myProc.status = "active";
            myProc.reclaims = 0;
            callback(null, myProc);
        }
    });
}
