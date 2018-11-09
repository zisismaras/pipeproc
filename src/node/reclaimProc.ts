import debug from "debug";
import LevelDOWN from "leveldown";
import {transaction} from "./transaction";
import {IProc} from "./proc";
import {disableProc} from "./resumeDisableProc";
const d = debug("pipeproc:node");

export function reclaimProc(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    procName: string,
    callback: (err?: Error|null, lastClaimedRange?: string) => void
): void {
    const myProc = activeProcs.find(p => p.name === procName);
    if (!myProc) {
        return callback(new Error("invalid_proc"));
    }
    if (myProc.status === "disabled") {
        return callback(new Error("proc_is_disabled"));
    }
    if (myProc.lastAckedRange === myProc.lastClaimedRange) {
        return callback(new Error("nothing_to_reclaim"));
    }
    d("reclaiming proc:", myProc.name);
    const tx = transaction(db);
    const prefix = `~~system~~#proc#${myProc.topic}#${myProc.name}#`;
    tx.add([{
        key: `${prefix}lastClaimedRange`,
        value: myProc.previousClaimedRange
    }, {
        key: `${prefix}reclaims`,
        value: `${myProc.reclaims + 1}`
    }]);
    tx.commitUpdate(function(err) {
        if (err) {
            callback(err);
        } else {
            myProc.lastClaimedRange = myProc.previousClaimedRange;
            myProc.reclaims = myProc.reclaims + 1;
            executeStrategy(db, activeProcs, myProc, function(strategyErr) {
                if (err) {
                    callback(strategyErr);
                } else {
                    callback(null, myProc.lastClaimedRange);
                }
            });
        }
    });
}

function executeStrategy(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    myProc: IProc,
    callback: (err?: Error) => void
) {
    if (myProc.onMaxReclaimsReached === "disable" &&
        myProc.reclaims >= myProc.maxReclaims && myProc.maxReclaims !== -1) {
        disableProc(db, activeProcs, myProc.name, function(err) {
            if (err) {
                callback(err);
            } else {
                callback();
            }
        });
    } else {
        callback();
    }
}
