import debug from "debug";
import LevelDOWN from "leveldown";
import {IProc, createProc} from "./proc";
import {IWorker} from "./workerManager";
import {sendMessageToWorker} from "./messaging";
import {
    prepareRegisterSystemProcsMessage,
    IPipeProcRegisterSystemProcsMessageReply
} from "../common/messages";
import {series, eachOfSeries} from "async";
import {transaction} from "./transaction";

const d = debug("pipeproc:node");

export interface ISystemProc {
    [index: string]: string | string[] | true | undefined;
    name: string;
    topic: string;
    inlineProcessor?: string;
    externalProcessor?: string;
    to: string | string[];
}

export function systemProc(
    db: LevelDOWN.LevelDown,
    activeProcs: IProc[],
    activeSystemProcs: ISystemProc[],
    activeWorkers: IWorker[],
    options: {
        name: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string,
        from: string | string[];
        to: string | string[];
        inlineProcessor?: string,
        externalProcessor?: string
    },
    callback: (err?: Error | null, myProc?: IProc | IProc[]) => void
): void {
    d("starting system proc creation...");
    let procsCreated: IProc[] = [];
    let systemProcsCreated: ISystemProc[] = [];
    series([
        function(cb) {
            if (!options.inlineProcessor && !options.externalProcessor) {
                return callback(new Error("no_valid_processor_provided"));
            }
            validateProcs(options.name, options.from, activeProcs, function(err, procs) {
                if (err) {
                    callback(err);
                } else if (procs) {
                    if (Array.isArray(procs)) {
                        if (procs.length === options.from.length) {
                            callback(null, procs);
                        } else {
                            options.from = (<string[]>options.from).filter(function(topic) {
                                return procs.filter(function(p) {
                                    return p.name !== `${options.name}__${topic}`;
                                }).length === 0;
                            });
                            cb();
                        }
                    } else {
                        callback(null, procs);
                    }
                } else {
                    cb();
                }
            });
        },
        function(cb) {
            createSystemProcs(db, options).commitUpdate(function(err, procs, systemProcs) {
                if (err) return callback(err);
                if (
                    Array.isArray(procs) && procs.length > 0 &&
                    Array.isArray(systemProcs) && systemProcs.length > 0
                ) {
                    procsCreated = <IProc[]>procs;
                    systemProcsCreated = <ISystemProc[]>systemProcs;
                    systemProcsCreated.forEach(sp => activeSystemProcs.push(sp));
                    procsCreated.forEach(p => activeProcs.push(p));
                    cb();
                } else if (procs && systemProcs && !Array.isArray(procs) && !Array.isArray(systemProcs)) {
                    procsCreated.push(<IProc>procs);
                    systemProcsCreated.push(<ISystemProc>systemProcs);
                    activeSystemProcs.push(<ISystemProc>systemProcs);
                    activeProcs.push(<IProc>procs);
                    cb();
                } else {
                    callback(new Error("proc_creation_failed"));
                }
            });
        },
        function(cb) {
            eachOfSeries(activeWorkers, function(worker, i, next) {
                d("sending systemProc to worker:", i);
                const msg = prepareRegisterSystemProcsMessage(procsCreated, systemProcsCreated);
                const listener = function(e: IPipeProcRegisterSystemProcsMessageReply) {
                    if (e.msgKey === msg.msgKey) {
                        worker.process.removeListener("message", listener);
                        if (e.type === "register_system_proc_ok") {
                            next();
                        } else {
                            d("Worker Error:", e.errStatus);
                            next(new Error(e.errStatus || "register_system_proc_uknown_error"));
                        }
                    }
                };
                worker.process.on("message", listener);
                sendMessageToWorker(msg, worker);
            }, cb);
        }
    ], function() {
        callback(null, (procsCreated.length === 1 ? procsCreated[0] : procsCreated));
    });
}

function createSystemProcs(
    db: LevelDOWN.LevelDown,
    options: {
        name: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string,
        from: string | string[];
        to: string | string[];
        inlineProcessor?: string,
        externalProcessor?: string
    }
) {
    const tx = transaction<IProc | ISystemProc>(db);
    d("creating...", options.from);
    if (Array.isArray(options.from)) {
        options.from.forEach(function(topic) {
            const procName = `${options.name}__${topic}`;
            d("creating", procName);
            const systemProcPrefix = `~~system~~#systemProc#${topic}#${procName}#`;
            tx.add(createProc(db, {
                name: procName,
                topic: topic,
                offset: options.offset,
                count: options.count,
                maxReclaims: options.maxReclaims,
                reclaimTimeout: options.reclaimTimeout,
                onMaxReclaimsReached: options.onMaxReclaimsReached
            }));
            tx.add({
                key: `${systemProcPrefix}name`,
                value: procName
            });
            if (options.inlineProcessor) {
                tx.add({
                    key: `${systemProcPrefix}inlineProcessor`,
                    value: options.inlineProcessor
                });
            } else if (options.externalProcessor) {
                tx.add({
                    key: `${systemProcPrefix}externalProcessor`,
                    value: options.externalProcessor
                });
            }
            let to = [];
            if (Array.isArray(options.to)) {
                to = options.to;
            } else {
                to.push(options.to);
            }
            to.forEach(function(toTopic, i) {
                tx.add({
                    key: `${systemProcPrefix}to#${i}`,
                    value: toTopic
                });
            });
            tx.done(function() {
                const result: {
                    name: string,
                    topic: string,
                    to: string | string[],
                    inlineProcessor?: string,
                    externalProcessor?: string
                } = {
                    name: procName,
                    topic: topic,
                    to: options.to
                };
                if (options.inlineProcessor) {
                    result.inlineProcessor = options.inlineProcessor;
                } else if (options.externalProcessor) {
                    result.externalProcessor = options.externalProcessor;
                }
                return result;
            }, "systemProcs");
        });
    } else {
        const procName = options.name;
        d("creating", procName);
        const systemProcPrefix = `~~system~~#systemProc#${options.from}#${procName}#`;
        tx.add(createProc(db, {
            name: procName,
            topic: options.from,
            offset: options.offset,
            count: options.count,
            maxReclaims: options.maxReclaims,
            reclaimTimeout: options.reclaimTimeout,
            onMaxReclaimsReached: options.onMaxReclaimsReached
        }));
        tx.add({
            key: `${systemProcPrefix}name`,
            value: procName
        });
        if (options.inlineProcessor) {
            tx.add({
                key: `${systemProcPrefix}inlineProcessor`,
                value: options.inlineProcessor
            });
        } else if (options.externalProcessor) {
            tx.add({
                key: `${systemProcPrefix}externalProcessor`,
                value: options.externalProcessor
            });
        }
        let to = [];
        if (Array.isArray(options.to)) {
            to = options.to;
        } else {
            to.push(options.to);
        }
        to.forEach(function(toTopic, i) {
            tx.add({
                key: `${systemProcPrefix}to#${i}`,
                value: toTopic
            });
        });
        tx.done(function() {
            const result: {
                name: string,
                topic: string,
                to: string | string[],
                inlineProcessor?: string,
                externalProcessor?: string
            } = {
                name: procName,
                topic: <string>options.from,
                to: options.to
            };
            if (options.inlineProcessor) {
                result.inlineProcessor = options.inlineProcessor;
            } else if (options.externalProcessor) {
                result.externalProcessor = options.externalProcessor;
            }
            return result;
        }, "systemProcs");
    }
    return tx;
}

export function validateProcs(
    name: string,
    from: string | string[],
    activeProcs: IProc[],
    callback: (err?: Error | null, myProc?: IProc | IProc[]) => void
): void {
    d("validating...");
    if (Array.isArray(from)) {
        let valid = true;
        const procs: IProc[] = [];
        from.forEach(function(topic) {
            const procName = `${name}__${topic}`;
            const myProc = activeProcs.find(p => p.name === procName);
            if (myProc && myProc.topic !== topic) {
                d(procName, "NOT_UNIQUE");
                valid = false;
            } else if (myProc && myProc.topic === topic) {
                d(procName, "OK_EXISTS");
                procs.push(myProc);
            } else {
                d(procName, "OK_NOT_EXISTS");
            }
        });
        if (valid) {
            callback(null, procs);
        } else {
            callback(new Error("proc_name_not_unique"));
        }
    } else {
        const myProc = activeProcs.find(p => p.name === name);
        if (myProc && myProc.topic !== from) {
            d(name, "NOT_UNIQUE");
            callback(new Error("proc_name_not_unique"));
        } else if (myProc && myProc.topic === from) {
            d(name, "OK_EXISTS");
            callback(null, myProc);
        } else {
            d(name, "OK_NOT_EXISTS");
            callback();
        }
    }
}
