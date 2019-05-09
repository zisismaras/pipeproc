import LevelDOWN from "leveldown";
import {ISystemState} from "./pipeProc";
import {IWorker} from "./workerManager";
export function runShutdownHooks(
    db: LevelDOWN.LevelDown,
    systemState: ISystemState,
    activeWorkers: IWorker[],
    callback: (err?: Error) => void
): void {
    activeWorkers.forEach(function(worker) {
        worker.process.kill("SIGTERM");
    });
    if (db && typeof db.close === "function") {
        db.close(function(err) {
            systemState.active = false;
            callback(err);
        });
    } else {
        systemState.active = false;
        callback();
    }
}
