import {LevelDown as LevelDOWN} from "leveldown";
import {ISystemState} from "./pipeProc";
import {IWorker} from "./workerManager";
import debug from "debug";

const d = debug("pipeproc:node");

export function runShutdownHooks(
    db: LevelDOWN,
    systemState: ISystemState,
    activeWorkers: IWorker[],
    callback: (err?: Error) => void
): void {
    activeWorkers.forEach(function(worker) {
        worker.monitor.stop();
        worker.process.kill("SIGTERM");
    });
    d("workers closed");
    if (db && typeof db.close === "function") {
        db.close(function(err) {
            d("db store closed");
            systemState.active = false;
            callback(err);
        });
    } else {
        d("db store closed");
        systemState.active = false;
        callback();
    }
}
