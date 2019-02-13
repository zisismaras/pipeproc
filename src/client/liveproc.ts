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
import debug from "debug";
const d = debug("pipeproc:client:liveproc");

type ChangesCb = (
    this: ILiveProc,
    err: null | Error,
    result: {id: string, body: object} | {id: string, body: object}[] | null,
    nextCb: () => void
) => void | Promise<void>;
export interface ILiveProc {
    changes: (cb: ChangesCb) => ILiveProc;
    inspect: () => Promise<IProc>;
    destroy: () => Promise<IProc>;
    disable: () => Promise<IProc>;
    resume: () => Promise<IProc>;
    reclaim: () => Promise<string>;
    ack: () => Promise<string>;
    ackCommit: (commitLog: ICommitLog) => void;
    cancel: () => Promise<void>;
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
    const name = `liveproc-${uuid()}`;
    d(`starting liveProc: ${name}`);
    let stop = false;
    const lp: ILiveProc = {
        changes: function(cb) {
            //start loop that calls changesFn and also locks by processing flag
            const strategy = new ExponentialStrategy({
                randomisationFactor: 0.5,
                initialDelay: 10,
                maxDelay: 10000,
                factor: 2
            });
            forever(function(next) {
                if (stop) return next("stop");
                if (processing) return setTimeout(next, strategy.next());
                processing = true;
                return getLog(function(err, result) {
                    processing  = false;
                    if (err) {
                        const nextCb = function() {
                            setTimeout(next, strategy.next());
                        };
                        const cbResult = cb.call(lp, err, null, nextCb);
                        if (cbResult instanceof Promise) {
                            cbResult.then(function() {
                                nextCb();
                            });
                        }
                    } else if (result) {
                        const nextCb = function() {
                            strategy.reset();
                            setImmediate(next);
                        };
                        const cbResult = cb.call(lp, null, result, nextCb);
                        if (cbResult instanceof Promise) {
                            cbResult.then(function() {
                                nextCb();
                            });
                        }
                    } else {
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
        },
        cancel: function() {
            d(`stopping liveProc: ${name}`);
            return this.disable().then(function() {
                stop = true;
            });
        }
    };
    function getLog(
        cb: (
            err: Error | null,
            log: {
                id: string;
                body: object;
            } | {
                id: string;
                body: object;
            }[] | undefined
        ) => void
    ) {
        proc(client, options.topic, {
            name: name,
            offset: options.mode === "live" ? "$>" : ">",
            count: options.count,
            onMaxReclaimsReached: "continue"
        }, function(err, log) {
            cb(err || null, log);
        });
    }

    return lp;
}
