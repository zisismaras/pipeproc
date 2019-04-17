import {
    IPipeProcMessage,
    IPipeProcWorkerInitMessage,
    IPipeProcRegisterSystemProcsMessage,
    prepareMessage
} from "../../common/messages";
import {PipeProc, ICommitLog} from "../../client";
import {IProc} from "../../node/proc";
import {series, forever} from "async";
import debug from "debug";
import { ISystemProc } from "../systemProc";
import {ExponentialStrategy} from "backoff";
const d = debug(`pipeproc:worker:${process.pid}`);

const pipeProcClient = PipeProc();

function sendMessageToNode(msg: IPipeProcMessage, cb?: () => void): void {
    if (process && typeof process.send === "function") {
        process.send(msg, cb);
    }
}

export interface IProcessorMap {
    [systemProcName: string]: (
        callback: (err?: Error | string | null) => void
    ) => void;
}

type ProcessorFn = (
    log: {
        id: string;
        body: object;
    } | {
        id: string;
        body: object;
    }[],
    done?: (
        err?: Error | null,
        data?: object | object[]
    ) => void
) => Promise<ICommitLog | ICommitLog[]> | void;

const processorMap: IProcessorMap = {};

process.on("message", function(e: IPipeProcWorkerInitMessage) {
    if (e.type === "worker_init") {
        pipeProcClient.connect({isWorker: true, namespace: e.data.namespace})
        .then(function(status) {
            sendMessageToNode(prepareMessage({
                type: "connected",
                msgKey: e.msgKey,
                data: {
                    status: status
                }
            }));
        })
        .catch(function(err) {
            sendMessageToNode(prepareMessage({
                type: "connection_error",
                msgKey: e.msgKey,
                errStatus: err.message
            }));
        });
    }
});

process.on("message", function(e: IPipeProcRegisterSystemProcsMessage) {
    if (e.type === "register_system_procs") {
        d("registering...");
        let constructionError: Error | null = null;
        e.data.procs.forEach(myProc => {
            if (constructionError) return;
            const err = constructProcExec(myProc, e.data.systemProcs);
            if (err) constructionError = err;
        });
        if (constructionError) {
            sendMessageToNode(prepareMessage({
                type: "register_system_proc_error",
                msgKey: e.msgKey,
                data: {
                    status: "error"
                },
                errStatus: (constructionError && (<Error>constructionError).message) || "proc_construction_error"
            }));
        } else {
            sendMessageToNode(prepareMessage({
                type: "register_system_proc_ok",
                msgKey: e.msgKey,
                data: {
                    status: "ok"
                }
            }));
            e.data.procs.forEach(myProc => {
                if (myProc.status !== "active") return;
                const strategy = new ExponentialStrategy({
                    randomisationFactor: 0.5,
                    initialDelay: 10,
                    maxDelay: 3000,
                    factor: 2
                });
                forever(function(next) {
                    if (myProc.status !== "active") return next(new Error("stop"));
                    processorMap[myProc.name](function(err) {
                        if (err) {
                            d(err);
                            setTimeout(next, strategy.next());
                        } else {
                            strategy.reset();
                            setImmediate(next);
                        }
                    });
                }, function() {});
            });
        }
    }
});

function constructProcExec(myProc: IProc, systemProcs: ISystemProc[]): Error | void {
    d("constructing", myProc.name);
    const mySystemProc = systemProcs.find(sp => sp.name === myProc.name);
    if (!mySystemProc) {
        return new Error("invalid_procs_passed");
    }
    let processorFn: ProcessorFn;
    try {
        if (mySystemProc.externalProcessor) {
            //tslint:disable non-literal-require
            processorFn = require(mySystemProc.externalProcessor);
            if (typeof processorFn !== "function") {
                processorFn = (<{default: ProcessorFn}>processorFn).default;
            }
            //tslint:enable
        }
        if (mySystemProc.inlineProcessor) {
            //tslint:disable no-function-constructor-with-string-args
            processorFn = <ProcessorFn>(new Function("log", "done", `
                return (${mySystemProc.inlineProcessor}).call(null, log, done);
            `));
            //tslint:enable
        }
    } catch (e) {
        return e;
    }
    processorMap[myProc.name] = function(callback) {
        let myLog: {
            id: string;
            body: object;
        } | {
            id: string;
            body: object;
        }[];
        let myCommitLog: ICommitLog | ICommitLog[];
        let shouldCommit = false;
        let processorErr: Error;
        let ackCommitErr: Error;
        series([
            function(cb) {
                pipeProcClient.proc(myProc.topic, myProc)
                .then(function(log) {
                    if ((log && !Array.isArray(log)) || (Array.isArray(log) && log.length > 0)) {
                        myLog = log;
                        cb();
                    } else {
                        cb("no_results");
                    }
                })
                .catch(cb);
            },
            function(cb) {
                try {
                    const myProcessorPromise = processorFn(myLog, function(err, data) {
                        if (err) {
                            processorErr = err;
                        } else if (
                            data && (
                                (Array.isArray(mySystemProc.to) && mySystemProc.to.length > 0) ||
                                mySystemProc.to
                            )) {
                            if (Array.isArray(mySystemProc.to) && mySystemProc.to.length > 0) {
                                myCommitLog = [];
                                mySystemProc.to.forEach(topic => {
                                    if (Array.isArray(data) && data.length > 0) {
                                        shouldCommit = true;
                                        data.forEach(dataBody => {
                                            (<ICommitLog[]>myCommitLog).push({
                                                topic: topic,
                                                body: dataBody
                                            });
                                        });
                                    } else {
                                        shouldCommit = true;
                                        (<ICommitLog[]>myCommitLog).push({
                                            topic: topic,
                                            body: data
                                        });
                                    }
                                });
                            } else {
                                if (Array.isArray(data) && data.length > 0) {
                                    myCommitLog = [];
                                    shouldCommit = true;
                                    data.forEach(dataBody => {
                                        (<ICommitLog[]>myCommitLog).push({
                                            topic: <string>mySystemProc.to,
                                            body: dataBody
                                        });
                                    });
                                } else {
                                    shouldCommit = true;
                                    myCommitLog = {
                                        topic: <string>mySystemProc.to,
                                        body: data
                                    };
                                }
                            }
                        }
                        cb();
                    });
                    if (myProcessorPromise instanceof Promise) {
                        myProcessorPromise.then(function(data) {
                            if (!data) return cb();
                            if (Array.isArray(mySystemProc.to) && mySystemProc.to.length > 0) {
                                myCommitLog = [];
                                mySystemProc.to.forEach(topic => {
                                    if (Array.isArray(data) && data.length > 0) {
                                        shouldCommit = true;
                                        data.forEach(dataBody => {
                                            (<ICommitLog[]>myCommitLog).push({
                                                topic: topic,
                                                body: dataBody
                                            });
                                        });
                                    } else {
                                        shouldCommit = true;
                                        (<ICommitLog[]>myCommitLog).push({
                                            topic: topic,
                                            body: data
                                        });
                                    }
                                });
                            } else {
                                if (Array.isArray(data) && data.length > 0) {
                                    myCommitLog = [];
                                    shouldCommit = true;
                                    data.forEach(dataBody => {
                                        (<ICommitLog[]>myCommitLog).push({
                                            topic: <string>mySystemProc.to,
                                            body: dataBody
                                        });
                                    });
                                } else {
                                    shouldCommit = true;
                                    myCommitLog = {
                                        topic: <string>mySystemProc.to,
                                        body: data
                                    };
                                }
                            }
                            cb();
                        }).catch(function(err) {
                            processorErr = err;
                            cb();
                        });
                    }
                } catch (e) {
                    cb(e);
                }
            },
            function(cb) {
                if (processorErr) {
                    pipeProcClient.reclaimProc(myProc.name)
                    .then(function() {
                        cb();
                    })
                    .catch(cb);
                } else {
                    setImmediate(cb);
                }
            },
            function(cb) {
                if (processorErr) {
                    setImmediate(cb);
                } else {
                    if (shouldCommit) {
                        pipeProcClient.ackCommit(myProc.name, myCommitLog)
                        .then(function() {
                            cb();
                        })
                        .catch(function(err) {
                            ackCommitErr = err;
                            cb();
                        });
                    } else {
                        pipeProcClient.ack(myProc.name)
                        .then(function() {
                            cb();
                        })
                        .catch(function(err) {
                            ackCommitErr = err;
                            cb();
                        });
                    }
                }
            },
            function(cb) {
                if (ackCommitErr) {
                    pipeProcClient.reclaimProc(myProc.name)
                    .then(function() {
                        cb();
                    })
                    .catch(cb);
                } else {
                    cb();
                }
            },
            function(cb) {
                if (processorErr) {
                    cb(processorErr);
                } else if (ackCommitErr) {
                    cb(ackCommitErr);
                } else {
                    cb();
                }
            }
        ], callback);
    };
}
