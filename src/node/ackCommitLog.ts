import debug from "debug";
import {LevelDown as LevelDOWN} from "leveldown";
import {IActiveTopics} from "./pipeProc";
import {preCommit} from "./commitLog";
import {prepareAck} from "./ack";
import {IProc} from "./proc";
import { transaction } from "./transaction";

const d = debug("pipeproc:node");

export function ackCommitLog(
    db: LevelDOWN,
    activeTopics: IActiveTopics,
    activeProcs: IProc[],
    procName: string,
    log: {topic: string; body: string} | {topic: string; body: string}[],
    callback: (err: Error|null, status: [string | undefined, string | string[]]) => void
): void {
    d("ack for proc:", procName);
    const myProc = activeProcs.find(p => p.name === procName);
    if (!myProc) {
        return callback(new Error("invalid_proc"), ["", ""]);
    }
    const creationTime = Date.now();
    const tx = transaction<string | number>(db);
    tx.add(prepareAck(db, myProc));

    d("acking:", myProc.lastClaimedRange);
    d("new log(s):\n%O", log);

    if (Array.isArray(log)) {
        log.forEach(l => tx.add(preCommit(db, activeTopics, l, creationTime)));
    } else {
        tx.add(preCommit(db, activeTopics, log, creationTime));
    }

    tx.commitUpdate(function(err, lastAckedAt, commit) {
        if (err) {
            callback(err, ["", ""]);
        } else if (lastAckedAt && (
            typeof commit === "string" || (Array.isArray(commit) && commit.length > 0))
        ) {
            myProc.lastAckedRange = myProc.lastClaimedRange;
            myProc.lastAckedAt = <number>lastAckedAt;
            myProc.reclaims = 0;
            if (Array.isArray(log)) {
                callback(null, [myProc.lastAckedRange, <string[]>commit]);
            } else {
                callback(null, [myProc.lastAckedRange, <string>commit]);
            }
        } else {
            callback(null, ["", ""]);
        }
    });
}
