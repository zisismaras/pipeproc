import debug from "debug";
import LevelDOWN from "leveldown";
import {IActiveTopics} from "./pipeProc";
import {transaction} from "./transaction";

const d = debug("pipeproc:node");

export function commitLog(
    db: LevelDOWN.LevelDown,
    activeTopics: IActiveTopics,
    log: {topic: string; body: string} | {topic: string; body: string}[],
    callback: (err?: Error|null, id?: string | string[]) => void
): void {
    d("new log(s):\n%O", log);
    const tx = transaction<string>(db);
    const creationTime = Date.now();

    if (Array.isArray(log)) {
        log.forEach(l => tx.add(preCommit(db, activeTopics, l, creationTime)));
    } else {
        tx.add(preCommit(db, activeTopics, log, creationTime));
    }

    tx.commitUpdate(function(err, commit) {
        if (err) {
            callback(err);
        } else if (
            typeof commit === "string" || (Array.isArray(commit) && commit.length > 0)
        ) {
            callback(null, commit);
        } else {
            callback();
        }
    });
}

export function preCommit(
    db: LevelDOWN.LevelDown,
    activeTopics: IActiveTopics,
    log: {topic: string; body: string},
    creationTime: number
) {
    const tx = transaction<string>(db);
    if (!activeTopics[log.topic]) {
        activeTopics[log.topic] = {
            createdAt: creationTime,
            currentTone: -1
        };
        tx.add([
            {key: `~~system~~#activeTopics#${log.topic}`, value: `${creationTime}`},
            {key: `~~system~~#currentTone#${log.topic}`, value: "-1"}
        ]);
    }
    const id = `${creationTime}-${activeTopics[log.topic].currentTone + 1}`;
    const key = `topic#${log.topic}#key#${id}`;
    const idKey = `~~internal~~#topic#${log.topic}#idKey#${activeTopics[log.topic].currentTone + 1}`;

    tx.add([{
        key: key,
        value: log.body
    }, {
        key: idKey,
        value: key
    }, {
        key: `~~system~~#currentTone#${log.topic}`,
        value: `${activeTopics[log.topic].currentTone + 1}`
    }]);
    activeTopics[log.topic].currentTone += 1;

    tx.done(function() {
        return id;
    }, "commits");

    return tx;
}
