process.env.DEBUG_COLORS = "true";
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
    waitForProcs,
    availableProc
} from "./actions";
import {
    createLiveProc,
    ILiveProc
} from "./liveproc";
import {IProc} from "../node/proc";
import {ConnectSocket} from "../socket/connect";
import {resolve as pathResolve} from "path";
import {existsSync} from "fs";
import {Monitor} from "forever-monitor";
import {ChildProcess} from "child_process";

//tslint:disable interface-name
declare module "forever-monitor" {
    interface Monitor {
        child: ChildProcess;
    }
}
//tslint:enable interface-name

export interface ICommitLog {
    topic: string;
    body: object;
}

type InlineProcessorFn = (
    result?: {id: string, body: object} | {id: string, body: object}[],
    done?: (err: Error | null, newLog?: {id: string, body: object} | {id: string, body: object}[]) => void
) => void;

export interface IPipeProcClient {
    pipeProcNode?: Monitor | {};
    connectSocket?: ConnectSocket;
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
            tls?: {
                server: {
                    key: string;
                    cert: string;
                    ca: string;
                },
                client: {
                    key: string;
                    cert: string;
                    ca: string;
                }
            }
            socket?: string,
            memory?: boolean,
            location?: string,
            workers?: number,
            workerConcurrency?: number,
            workerRestartAfter?: number,
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
            tls?: {
                key: string;
                cert: string;
                ca: string;
            } | false,
            socket?: string,
            isWorker?: boolean,
            timeout?: number
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
    availableProc(
        procList: {
            name: string,
            topic: string,
            offset: string,
            count?: number,
            maxReclaims?: number,
            reclaimTimeout?: number,
            onMaxReclaimsReached?: string
        }[]
    ): Promise<undefined | {
        procName?: string,
        log?: {id: string, body: object} | {id: string, body: object}[]
    }>;
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
            processor: string | InlineProcessorFn;
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
            let address = "";
            if (options && options.socket && typeof options.socket === "string") {
                address = options.socket;
            }
            if (options && options.tcp) {
                if (!options.tcp.host || !options.tcp.port) {
                    return Promise.reject(new Error("tcp connection needs a host and a port"));
                }
            }
            let workers: number;
            if (options && typeof options.workers === "number" && options.workers >= 0) {
                workers = options.workers;
            } else {
                workers = 1;
            }
            let workerConcurrency: number;
            if (options && typeof options.workerConcurrency === "number" && options.workerConcurrency >= 1) {
                workerConcurrency = options.workerConcurrency;
            } else {
                workerConcurrency = 1;
            }
            let workerRestartAfter: number;
            if (options && typeof options.workerRestartAfter === "number" && options.workerRestartAfter >= 0) {
                workerRestartAfter = options.workerRestartAfter;
            } else {
                workerRestartAfter = 0;
            }
            let tls: {
                server: {
                    key: string;
                    cert: string;
                    ca: string;
                },
                client: {
                    key: string;
                    cert: string;
                    ca: string;
                }
            } | false;
            if (options && options.tls) {
                if (!options.tls.server || !options.tls.client) {
                    return Promise.reject(new Error("tls options require a server and client configuration"));
                }
                if (!existsSync(options.tls.server.ca)) {
                    return Promise.reject(new Error("tls options require a server ca"));
                }
                if (!existsSync(options.tls.server.cert)) {
                    return Promise.reject(new Error("tls options require a server cert"));
                }
                if (!existsSync(options.tls.server.key)) {
                    return Promise.reject(new Error("tls options require a server key"));
                }
                if (!existsSync(options.tls.client.ca)) {
                    return Promise.reject(new Error("tls options require a client ca"));
                }
                if (!existsSync(options.tls.client.cert)) {
                    return Promise.reject(new Error("tls options require a client cert"));
                }
                if (!existsSync(options.tls.client.key)) {
                    return Promise.reject(new Error("tls options require a client key"));
                }
                tls = {
                    server: {
                        ca: pathResolve(options.tls.server.ca),
                        cert: pathResolve(options.tls.server.cert),
                        key: pathResolve(options.tls.server.key)
                    },
                    client: {
                        ca: pathResolve(options.tls.client.ca),
                        cert: pathResolve(options.tls.client.cert),
                        key: pathResolve(options.tls.client.key)
                    }
                };
            } else {
                tls = false;
            }
            return new Promise(function(resolve, reject) {
                spawn(pipeProcClient, {
                    address: address,
                    namespace: namespace,
                    tcp: (options && options.tcp) || false,
                    memory: (options && options.memory) || false,
                    location: (options && options.location) || "./pipeproc_data",
                    workers: workers,
                    workerConcurrency: workerConcurrency,
                    workerRestartAfter: workerRestartAfter,
                    gc: (options && options.gc) || undefined,
                    tls: tls
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
            let address = "";
            if (options && options.socket && typeof options.socket === "string") {
                address = options.socket;
            }
            if (options && options.tcp) {
                if (!options.tcp.host || !options.tcp.port) {
                    return Promise.reject(new Error("tcp connection needs a host and a port"));
                }
            }
            let tls: {
                key: string;
                cert: string;
                ca: string;
            } | false;
            if (options && options.tls) {
                if (!existsSync(options.tls.ca)) {
                    return Promise.reject(new Error("tls connect options require a client ca"));
                }
                if (!existsSync(options.tls.cert)) {
                    return Promise.reject(new Error("tls connect options require a client cert"));
                }
                if (!existsSync(options.tls.key)) {
                    return Promise.reject(new Error("tls connect options require a client key"));
                }
                tls = {
                    ca: pathResolve(options.tls.ca),
                    cert: pathResolve(options.tls.cert),
                    key: pathResolve(options.tls.key)
                };
            } else {
                tls = false;
            }
            return new Promise(function(resolve, reject) {
                connect(pipeProcClient, {
                    address: address,
                    namespace: namespace,
                    tcp: (options && options.tcp) || false,
                    isWorker: (options && options.isWorker) || false,
                    tls: tls,
                    timeout: (options && options.timeout) || 1000
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
        availableProc: function(procList) {
            return new Promise(function(resolve, reject) {
                availableProc(pipeProcClient, procList, function(err, result) {
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
