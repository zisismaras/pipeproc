/// <reference types="../typings/node-ipc" />
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
    reclaimProc
} from "./actions";
import {
    createLiveProc,
    ILiveProc
} from "./liveproc";
import {IProc} from "../node/proc";
import {IPC} from "node-ipc";
import {ChildProcess} from "child_process";
import {cpus} from "os";

export interface ICommitLog {
    topic: string;
    body: object;
}

export interface IPipeProcClient {
    namespace: string;
    pipeProcNode?: ChildProcess | {};
    ipc?: IPC;
    messageMap: {
        //tslint:disable no-any
        [key: string]: (e: any) => void;
        //tslint:enable
    };
    commit(
        commitLog: ICommitLog | ICommitLog[],
        callback?: (err?: null | Error, logId?: string | string[]) => void
    ): Promise<string | string[]> | void;
    spawn(
        options?: {
            namespace?: string,
            memory?: boolean,
            location?: string,
            workers?: number,
            gc?: {minPruneTime?: number, interval?: number} | boolean
        },
        callback?: (err?: null | Error, status?: string) => void
    ): Promise<string> | void;
    connect(
        options?: {
            namespace?: string,
            isWorker?: boolean
        },
        callback?: (err?: null | Error, status?: string) => void
    ): Promise<string> | void;
    shutdown(
        callback?: (err?: null | Error, status?: string) => void
    ): Promise<string> | void;
    range(
        topic: string,
        options?: {
            start?: string,
            end?: string,
            limit?: number,
            exclusive?: boolean
        },
        callback?: (err?: null | Error, result?: {id: string, body: object}[]) => void
    ): Promise<{id: string, body: object}[]> | void;
    revrange(
        topic: string,
        options?: {
            start?: string,
            end?: string,
            limit?: number,
            exclusive?: boolean
        },
        callback?: (err?: null | Error, result?: {id: string, body: object}[]) => void
    ): Promise<{id: string, body: object}[]> | void;
    length(
        topic: string,
        callback?: (err?: null | Error, length?: number) => void
    ): Promise<number> | void;
    proc(
        topic: string,
        options: {
            name: string,
            offset: string,
            count?: number,
            maxReclaims?: number,
            reclaimTimeout?: number,
            onMaxReclaimsReached?: string
        },
        callback?: (err?: null | Error, result?: {id: string, body: object} | {id: string, body: object}[]) => void
    ): Promise<null | {id: string, body: object} | {id: string, body: object}[]> | void;
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
        },
        callback?: (err?: Error | null, myProc?: IProc | IProc[]) => void
    ): Promise<IProc | IProc[]> | void;
    ack(
        procName: string,
        callback?: (err?: null | Error, logId?: string) => void
    ): Promise<string> | void;
    ackCommit(
        procName: string,
        commitLog: ICommitLog | ICommitLog[],
        callback?: (err?: null | Error, result?: {ackedLogId: string, id: string | string[]}) => void
    ): Promise<{ackedLogId: string, id: string | string[]}> | void;
    inspectProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc> | void;
    destroyProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc> | void;
    disableProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc> | void;
    resumeProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc> | void;
    reclaimProc(
        procName: string,
        callback?: (err?: null | Error, lastClaimedRange?: string) => void
    ): Promise<string> | void;
    liveProc(
        options: {
            topic: string,
            mode: "live" | "all",
            count?: number
        }
    ): ILiveProc;
}

//tslint:disable function-name
export function PipeProc(): IPipeProcClient {
//tslint:enable function-name
    const pipeProcClient: IPipeProcClient = {
        namespace: "default",
        messageMap: {},
        spawn: function(options, callback) {
            var cb = callback;
            var opts: {
                namespace?: string,
                memory?: boolean,
                location?: string,
                workers?: number,
                gc?: {minPruneTime?: number, interval?: number} | boolean
            };
            if (typeof options === "function") { //only callback passed
                cb = options;
                opts = {};
            } else if (options && typeof options === "object") { //options passed
                opts = options;
            } else { //nothing passed
                opts = {};
            }
            if (opts.namespace && typeof opts.namespace === "string") {
                this.namespace = opts.namespace;
            }
            let workers: number;
            if ((opts.workers && opts.workers >= 0) || opts.workers === 0) {
                workers = opts.workers;
            } else {
                workers = cpus().length;
            }
            if (typeof cb === "function") {
                return spawn(pipeProcClient, {
                    memory: opts.memory || false,
                    location: opts.location || "./pipeproc_data",
                    workers: workers,
                    gc: opts.gc
                }, cb);
            } else {
                return new Promise(function(resolve, reject) {
                    spawn(pipeProcClient, {
                        memory: opts.memory || false,
                        location: opts.location || "./pipeproc_data",
                        workers: workers,
                        gc: opts.gc
                    }, function(err, status) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(status);
                        }
                    });
                });
            }
        },
        connect: function(options, callback) {
            var cb = callback;
            var opts: {
                namespace?: string,
                isWorker?: boolean
            };
            if (typeof options === "function") { //only callback passed
                cb = options;
                opts = {};
            } else if (options && typeof options === "object") { //options passed
                opts = options;
            } else { //nothing passed
                opts = {};
            }
            if (opts.namespace && typeof opts.namespace === "string") {
                this.namespace = opts.namespace;
            }
            if (typeof cb === "function") {
                return connect(pipeProcClient, {
                    isWorker: opts.isWorker || false
                }, cb);
            } else {
                return new Promise(function(resolve, reject) {
                    connect(pipeProcClient, {
                        isWorker: opts.isWorker || false
                    }, function(err, status) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(status);
                        }
                    });
                });
            }
        },
        shutdown: function(callback) {
            if (typeof callback === "function") {
                return shutdown(pipeProcClient, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    shutdown(pipeProcClient, function(err, status) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(status);
                        }
                    });
                });
            }
        },
        range: function(topic, options, callback) {
            var cb = callback;
            var opts: {
                start?: string;
                end?: string;
                limit?: number;
                exclusive?: boolean;
            };
            if (typeof options === "function") { //only callback passed
                cb = options;
                opts = {};
            } else if (options && typeof options === "object") { //options passed
                opts = options;
            } else { //nothing passed
                opts = {};
            }
            if (typeof cb === "function") {
                return range(pipeProcClient, topic, {
                    start: opts.start || "",
                    end: opts.end || "",
                    limit: opts.limit || -1,
                    exclusive: opts.exclusive || false,
                    reverse: false
                }, cb);
            } else {
                return new Promise(function(resolve, reject) {
                    range(pipeProcClient, topic, {
                        start: opts.start || "",
                        end: opts.end || "",
                        limit: opts.limit || -1,
                        exclusive: opts.exclusive || false,
                        reverse: false
                    }, function(err, results) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(results);
                        }
                    });
                });
            }
        },
        revrange: function(topic, options, callback) {
            var cb = callback;
            var opts: {
                start?: string;
                end?: string;
                limit?: number;
                exclusive?: boolean;
            };
            if (typeof options === "function") { //only callback passed
                cb = options;
                opts = {};
            } else if (options && typeof options === "object") { //options passed
                opts = options;
            } else { //nothing passed
                opts = {};
            }
            if (typeof cb === "function") {
                return range(pipeProcClient, topic, {
                    start: opts.start || "",
                    end: opts.end || "",
                    limit: opts.limit || -1,
                    exclusive: opts.exclusive || false,
                    reverse: true
                }, cb);
            } else {
                return new Promise(function(resolve, reject) {
                    range(pipeProcClient, topic, {
                        start: opts.start || "",
                        end: opts.end || "",
                        limit: opts.limit || -1,
                        exclusive: opts.exclusive || false,
                        reverse: true
                    }, function(err, results) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(results);
                        }
                    });
                });
            }
        },
        length: function(topic, callback) {
            if (typeof callback === "function") {
                return range(pipeProcClient, topic, {
                    start: "",
                    end: "",
                    limit: 1,
                    exclusive: false,
                    reverse: true
                }, function(err, results) {
                    if (err) {
                        callback(err);
                    } else {
                        if (results && results[0]) {
                            const l = parseInt(results[0].id.split("-")[1]) + 1;
                            callback(null, l);
                        } else {
                            callback(null, 0);
                        }
                    }
                });
            } else {
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
            }
        },
        commit: function(commitLog, callback) {
            if (typeof callback === "function") {
                return commit(pipeProcClient, commitLog, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    commit(pipeProcClient, commitLog, function(err, logId) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(logId);
                        }
                    });
                });
            }
        },
        proc: function(topic, options, callback) {
            if (typeof callback === "function") {
                return proc(pipeProcClient, topic, options, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    proc(pipeProcClient, topic, options, function(err, result) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(result);
                        }
                    });
                });
            }
        },
        systemProc: function(options, callback) {
            if (typeof callback === "function") {
                return systemProc(pipeProcClient, options, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    systemProc(pipeProcClient, options, function(err, myProc) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(myProc);
                        }
                    });
                });
            }
        },
        ack: function(procName, callback) {
            if (typeof callback === "function") {
                return ack(pipeProcClient, procName, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    ack(pipeProcClient, procName, function(err, logId) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(logId);
                        }
                    });
                });
            }
        },
        ackCommit: function(procName, commitLog, callback) {
            if (typeof callback === "function") {
                return ackCommit(pipeProcClient, procName, commitLog, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    ackCommit(pipeProcClient, procName, commitLog, function(err, results) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(results);
                        }
                    });
                });
            }
        },
        inspectProc: function(procName, callback) {
            if (typeof callback === "function") {
                return inspectProc(pipeProcClient, procName, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    inspectProc(pipeProcClient, procName, function(err, myProc) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(myProc);
                        }
                    });
                });
            }
        },
        destroyProc: function(procName, callback) {
            if (typeof callback === "function") {
                return destroyProc(pipeProcClient, procName, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    destroyProc(pipeProcClient, procName, function(err, myProc) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(myProc);
                        }
                    });
                });
            }
        },
        disableProc: function(procName, callback) {
            if (typeof callback === "function") {
                return disableProc(pipeProcClient, procName, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    disableProc(pipeProcClient, procName, function(err, myProc) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(myProc);
                        }
                    });
                });
            }
        },
        resumeProc: function(procName, callback) {
            if (typeof callback === "function") {
                return resumeProc(pipeProcClient, procName, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    resumeProc(pipeProcClient, procName, function(err, myProc) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(myProc);
                        }
                    });
                });
            }
        },
        reclaimProc: function(procName, callback) {
            if (typeof callback === "function") {
                return reclaimProc(pipeProcClient, procName, callback);
            } else {
                return new Promise(function(resolve, reject) {
                    reclaimProc(pipeProcClient, procName, function(err, lastClaimedRange) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(lastClaimedRange);
                        }
                    });
                });
            }
        },
        liveProc: function(options) {
            return createLiveProc(pipeProcClient, options);
        }
    };

    return pipeProcClient;
}
