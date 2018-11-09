import LevelDOWN from "leveldown";
import {ISystemState} from "./pipeProc";
export function runShutdownHooks(
    db: LevelDOWN.LevelDown,
    systemState: ISystemState,
    callback: (err?: Error) => void
): void {
    db.close(function(err) {
        systemState.active = false;
        callback(err);
    });
}
