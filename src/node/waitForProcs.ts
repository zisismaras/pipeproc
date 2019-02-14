import {IProc} from "./proc";
import {IActiveTopics} from "./pipeProc";
import {every, forever} from "async";

export function waitForProcs(
    activeTopics: IActiveTopics,
    registeredProcs: IProc[],
    procFilter: string[],
    callback: () => void
): void {
    forever(function(next) {
        let procsToCheck: IProc[];
        if (procFilter.length > 0) {
            procsToCheck = registeredProcs.
                filter(p => p.status === "active" && activeTopics[p.topic] && procFilter.indexOf(p.name) > -1);
        } else {
            procsToCheck = registeredProcs.
                filter(p => p.status === "active" && activeTopics[p.topic]);
        }
        every(procsToCheck, function(proc, nextProc) {
            const currentTone = activeTopics[proc.topic].currentTone;
            let procTone: number;
            if (proc.lastAckedRange.split("..")[1]) {
                procTone = parseInt(proc.lastAckedRange.split("..")[1].split("-")[1]);
            } else {
                return nextProc(undefined, false);
            }
            if (currentTone === procTone) {
                nextProc(undefined, true);
            } else {
                nextProc(undefined, false);
            }
        }, function(_err, allFlushed) {
            if (allFlushed) {
                next("stop");
            } else {
                setTimeout(function() {
                    next();
                }, 20);
            }
        });
    }, function() {
        callback();
    });
}
