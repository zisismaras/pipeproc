import {
    ack,
    ackCommit,
    proc,
    inspectProc,
    destroyProc,
    disableProc,
    resumeProc,
    reclaimProc
} from "./actions";
import {IPipeProcClient, ICommitLog} from ".";
import {IProc} from "../node/proc";
import {v4 as uuid} from "uuid";
import {ExponentialStrategy} from "backoff";
import {forever} from "async";
// import debug from "debug";
// const d = debug("pipeproc:client:liveproc");

type ChangesCb = (
    this: ILiveProc,
    err?: null | Error, result?: {id: string, body: object} | {id: string, body: object}[]
) => void;
export interface ILiveProc {
    changes: (cb: ChangesCb) => ILiveProc;
    inspect: () => Promise<IProc>;
    destroy: () => Promise<IProc>;
    disable: () => Promise<IProc>;
    resume: () => Promise<IProc>;
    reclaim: () => Promise<string>;
    ack: () => Promise<string>;
    ackCommit: (commitLog: ICommitLog) => void;
}
export function createLiveProc(
    client: IPipeProcClient,
    options: {
        topic: string,
        mode: "live" | "all",
        count?: number
    }
): ILiveProc {
    let processing = false;
    let active = false;
    const name = `liveproc-${uuid()}`;
    const lp: ILiveProc = {
        changes: function(cb) {
            if (active) return cb.call(lp, new Error("liveproc_already_active"));
            //start loop that calls changesFn and also locks by processing flag
            const strategy = new ExponentialStrategy({
                randomisationFactor: 0.5,
                initialDelay: 10,
                maxDelay: 10000,
                factor: 2
            });
            forever(function(next) {
                if (processing) return setTimeout(next, strategy.next());
                processing = true;
                return changesFn(function(err, result) {
                    processing  = false;
                    if (err) {
                        cb.call(lp, err);
                        setTimeout(next, strategy.next());
                    } else if (result) {
                        cb.call(lp, null, result);
                        strategy.reset();
                        setImmediate(next);
                    } else {
                        cb.call(lp);
                        setTimeout(next, strategy.next());
                    }
                });
            }, function() {});

            return lp;
        },
        inspect: function() {
            return new Promise(function(resolve, reject) {
                inspectProc(client, name, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(myProc);
                    }
                });
            });
        },
        destroy: function() {
            return new Promise(function(resolve, reject) {
                destroyProc(client, name, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        processing = false;
                        resolve(myProc);
                    }
                });
            });
        },
        disable: function() {
            return new Promise(function(resolve, reject) {
                disableProc(client, name, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        processing = false;
                        resolve(myProc);
                    }
                });
            });
        },
        resume: function() {
            return new Promise(function(resolve, reject) {
                resumeProc(client, name, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        processing = false;
                        resolve(myProc);
                    }
                });
            });
        },
        reclaim: function() {
            return new Promise(function(resolve, reject) {
                reclaimProc(client, name, function(err, lastClaimedRange) {
                    if (err) {
                        reject(err);
                    } else {
                        processing = false;
                        resolve(lastClaimedRange);
                    }
                });
            });
        },
        ack: function() {
            return new Promise(function(resolve, reject) {
                ack(client, name, function(err, logId) {
                    if (err) {
                        reject(err);
                    } else {
                        processing = false;
                        resolve(logId);
                    }
                });
            });
        },
        ackCommit: function(commitLog) {
            return new Promise(function(resolve, reject) {
                ackCommit(client, name, commitLog, function(err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        processing = false;
                        resolve(result);
                    }
                });
            });
        }
    };
    function changesFn(cb: ChangesCb) {
        active = true;
        proc(client, options.topic, {
            name: name,
            offset: options.mode === "live" ? "$>" : ">",
            count: options.count,
            onMaxReclaimsReached: "continue"
        }, function(err, pr) {
            cb.call(lp, err, pr);
        });
    }

    return lp;
}
