import {spawn, shutdown, connect} from "./process";
import {
    commit,
    range,
    ack,
    ackCommit,
    proc,
    systemProc,
    inspectProc,
    destroyProc,
    disableProc,
    resumeProc,
    reclaimProc,
    waitForProcs
} from "./actions";
import {
    createLiveProc,
    ILiveProc
} from "./liveproc";
import {IProc} from "../node/proc";
import {ChildProcess} from "child_process";
import {Socket} from "zeromq";

export interface ICommitLog {
    topic: string;
    body: object;
}

export interface IPipeProcClient {
    pipeProcNode?: ChildProcess | {};
    ipc?: Socket;
    messageMap: {
        //tslint:disable no-any
        [key: string]: (e: any) => void;
        //tslint:enable
    };
    commit(
        commitLog: ICommitLog | ICommitLog[]
    ): Promise<string | string[]>;
    spawn(
        options?: {
            namespace?: string,
            tcp?: {
                host: string,
                port: number
            },
            memory?: boolean,
            location?: string,
            workers?: number,
            gc?: {minPruneTime?: number, interval?: number} | boolean
        }
    ): Promise<string>;
    connect(
        options?: {
            namespace?: string,
            tcp?: {
                host: string,
                port: number
            },
            isWorker?: boolean
        }
    ): Promise<string>;
    shutdown(): Promise<string>;
    range(
        topic: string,
        options?: {
            start?: string,
            end?: string,
            limit?: number,
            exclusive?: boolean
        }
    ): Promise<{id: string, body: object}[]>;
    revrange(
        topic: string,
        options?: {
            start?: string,
            end?: string,
            limit?: number,
            exclusive?: boolean
        }
    ): Promise<{id: string, body: object}[]>;
    length(
        topic: string
    ): Promise<number>;
    proc(
        topic: string,
        options: {
            name: string,
            offset: string,
            count?: number,
            maxReclaims?: number,
            reclaimTimeout?: number,
            onMaxReclaimsReached?: string
        }
    ): Promise<null | {id: string, body: object} | {id: string, body: object}[]>;
    systemProc(
        options: {
            name: string,
            offset: string,
            count?: number,
            maxReclaims?: number,
            reclaimTimeout?: number,
            onMaxReclaimsReached?: string,
            from: string | string[],
            to?: string | string[],
            processor: string | ((result?: {id: string, body: object} | {id: string, body: object}[]) => void)
        }
    ): Promise<IProc | IProc[]>;
    ack(
        procName: string
    ): Promise<string>;
    ackCommit(
        procName: string,
        commitLog: ICommitLog | ICommitLog[]
    ): Promise<{ackedLogId: string, id: string | string[]}>;
    inspectProc(
        procName: string
    ): Promise<IProc>;
    destroyProc(
        procName: string
    ): Promise<IProc>;
    disableProc(
        procName: string
    ): Promise<IProc>;
    resumeProc(
        procName: string
    ): Promise<IProc>;
    reclaimProc(
        procName: string
    ): Promise<string>;
    liveProc(
        options: {
            topic: string,
            mode: "live" | "all",
            count?: number
        }
    ): ILiveProc;
    waitForProcs(
        procs?: string | string[]
    ): Promise<void>;
}

//tslint:disable function-name
export function PipeProc(): IPipeProcClient {
//tslint:enable function-name
    const pipeProcClient: IPipeProcClient = {
        messageMap: {},
        spawn: function(options) {
            let namespace = "default";
            if (options && options.namespace && typeof options.namespace === "string") {
                namespace = options.namespace;
            }
            if (options && options.tcp) {
                if (!options.tcp.host || !options.tcp.port) {
                    return Promise.reject(new Error("tcp connection needs a host and a port"));
                }
                if (options.namespace) {
                    return Promise.reject(new Error("cannot use both an ipc namespace and a tcp connection"));
                }
            }
            if (process.platform === "win32" && (!options || !options.tcp)) {
                return Promise.reject(new Error("only the ipc interface is available on windows"));
            }
            let workers: number;
            if (options && typeof options.workers === "number" && options.workers >= 0) {
                workers = options.workers;
            } else {
                workers = 1;
            }
            return new Promise(function(resolve, reject) {
                spawn(pipeProcClient, {
                    memory: (options && options.memory) || false,
                    location: (options && options.location) || "./pipeproc_data",
                    workers: workers,
                    namespace: namespace,
                    tcp: (options && options.tcp) || false,
                    gc: (options && options.gc) || undefined
                }, function(err, status) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(status);
                    }
                });
            });
        },
        connect: function(options) {
            let namespace = "default";
            if (options && options.namespace && typeof options.namespace === "string") {
                namespace = options.namespace;
            }
            if (options && options.tcp) {
                if (!options.tcp.host || !options.tcp.port) {
                    return Promise.reject(new Error("tcp connection needs a host and a port"));
                }
                if (options.namespace) {
                    return Promise.reject(new Error("cannot use both an ipc namespace and a tcp connection"));
                }
            }
            if (process.platform === "win32" && (!options || !options.tcp)) {
                return Promise.reject(new Error("only the ipc interface is available on windows"));
            }
            return new Promise(function(resolve, reject) {
                connect(pipeProcClient, {
                    isWorker: (options && options.isWorker) || false,
                    namespace: namespace,
                    tcp: (options && options.tcp) || false
                }, function(err, status) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(status);
                    }
                });
            });
        },
        shutdown: function() {
            return new Promise(function(resolve, reject) {
                shutdown(pipeProcClient, function(err, status) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(status);
                    }
                });
            });
        },
        range: function(topic, options) {
            return new Promise(function(resolve, reject) {
                range(pipeProcClient, topic, {
                    start: (options && options.start) || "",
                    end: (options && options.end) || "",
                    limit: (options && options.limit) || -1,
                    exclusive: (options && options.exclusive) || false,
                    reverse: false
                }, function(err, results) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(results);
                    }
                });
            });
        },
        revrange: function(topic, options) {
            return new Promise(function(resolve, reject) {
                range(pipeProcClient, topic, {
                    start: (options && options.start) || "",
                    end: (options && options.end) || "",
                    limit: (options && options.limit) || -1,
                    exclusive: (options && options.exclusive) || false,
                    reverse: true
                }, function(err, results) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(results);
                    }
                });
            });
        },
        length: function(topic) {
            return new Promise(function(resolve, reject) {
                range(pipeProcClient, topic, {
                    start: "",
                    end: "",
                    limit: 1,
                    exclusive: false,
                    reverse: true
                }, function(err, results) {
                    if (err) {
                        reject(err);
                    } else {
                        if (results && results[0]) {
                            const l = parseInt(results[0].id.split("-")[1]) + 1;
                            resolve(l);
                        } else {
                            resolve(0);
                        }
                    }
                });
            });
        },
        commit: function(commitLog) {
            return new Promise(function(resolve, reject) {
                commit(pipeProcClient, commitLog, function(err, logId) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(logId);
                    }
                });
            });
        },
        proc: function(topic, options) {
            return new Promise(function(resolve, reject) {
                proc(pipeProcClient, topic, options, function(err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result);
                    }
                });
            });
        },
        systemProc: function(options) {
            return new Promise(function(resolve, reject) {
                systemProc(pipeProcClient, options, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(myProc);
                    }
                });
            });
        },
        ack: function(procName) {
            return new Promise(function(resolve, reject) {
                ack(pipeProcClient, procName, function(err, logId) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(logId);
                    }
                });
            });
        },
        ackCommit: function(procName, commitLog) {
            return new Promise(function(resolve, reject) {
                ackCommit(pipeProcClient, procName, commitLog, function(err, results) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(results);
                    }
                });
            });
        },
        inspectProc: function(procName) {
            return new Promise(function(resolve, reject) {
                inspectProc(pipeProcClient, procName, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(myProc);
                    }
                });
            });
        },
        destroyProc: function(procName) {
            return new Promise(function(resolve, reject) {
                destroyProc(pipeProcClient, procName, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(myProc);
                    }
                });
            });
        },
        disableProc: function(procName) {
            return new Promise(function(resolve, reject) {
                disableProc(pipeProcClient, procName, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(myProc);
                    }
                });
            });
        },
        resumeProc: function(procName) {
            return new Promise(function(resolve, reject) {
                resumeProc(pipeProcClient, procName, function(err, myProc) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(myProc);
                    }
                });
            });
        },
        reclaimProc: function(procName) {
            return new Promise(function(resolve, reject) {
                reclaimProc(pipeProcClient, procName, function(err, lastClaimedRange) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(lastClaimedRange);
                    }
                });
            });
        },
        liveProc: function(options) {
            return createLiveProc(pipeProcClient, options);
        },
        waitForProcs: function(procs) {
            var procFilter: string[];
            if (Array.isArray(procs)) {
                procFilter = procs;
            } else if (typeof procs === "string") {
                procFilter = [procs];
            } else {
                procFilter = [];
            }
            return new Promise(function(resolve, reject) {
                waitForProcs(pipeProcClient, procFilter, function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }
    };

    return pipeProcClient;
}
