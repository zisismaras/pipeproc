import debug from "debug";
import LevelDOWN from "leveldown";
import {IProc} from "./proc";
import {transaction} from "./transaction";
import {forever, setImmediate as asyncImmediate} from "async";
const d = debug("pipeproc:node");

export function destroyProc(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    procName: string,
    callback: (err?: Error|null, proc?: IProc) => void
): void {
    const myProc = activeProcs.find(p => p.name === procName);
    if (!myProc) {
        return callback(new Error("invalid_proc"));
    }
    d("deleting proc:", procName);
    const iterator = db.iterator({
        gte: `~~system~~#proc#${myProc.topic}#${myProc.name}#`,
        values: false,
        keyAsBuffer: false,
        limit: -1
    });
    const keys: string[] = [];
    forever(function(next) {
        iterator.next(function(err, key) {
            if (err) return next(err);
            if (!key) return next("stop");
            keys.push(key);
            asyncImmediate(next);
        });
    }, function(status: {message?: string} | string | undefined) {
        if (!status || typeof status === "string") {
            iterator.end(function(iteratorEndErr) {
                if (iteratorEndErr) return callback(iteratorEndErr);
                const tx = transaction(db);
                tx.add(keys.map(key => {
                    return {key: key};
                }));
                tx.commitDelete(function(err) {
                    if (err) {
                        callback(err);
                    } else {
                        const procIndex = activeProcs.indexOf(myProc);
                        activeProcs.splice(procIndex, 1);
                        callback(null, myProc);
                    }
                });
            });
        } else {
            callback(new Error(status.message));
        }
    });
}
