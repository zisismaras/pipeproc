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
export interface ICommitLog {
    topic: string;
    body: object;
}

export interface IPipeProcClient {
    pipeProcNode?: NodeJS.Process | {};
    ipc?: IPC;
    messageMap: {
        //tslint:disable no-any
        [key: string]: (e: any) => void;
        //tslint:enable
    };
    commit(
        commitLog: ICommitLog | ICommitLog[],
        callback?: (err?: null | Error, logId?: string | string[]) => void
    ): Promise<string | string[]>;
    spawn(
        options?: {memory?: boolean, location?: string, workers?: number},
        callback?: (err?: null | Error, status?: string) => void
    ): Promise<string>;
    connect(
        options?: {isWorker?: boolean},
        callback?: (err?: null | Error, status?: string) => void
    ): Promise<string>;
    shutdown(
        callback?: (err?: null | Error, status?: string) => void
    ): Promise<string>;
    range(
        topic: string,
        options?: {
            start?: string,
            end?: string,
            limit?: number,
            exclusive?: boolean
        },
        callback?: (err?: null | Error, result?: {id: string, body: object}[]) => void
    ): Promise<{id: string, body: object}[]>;
    revrange(
        topic: string,
        options?: {
            start?: string,
            end?: string,
            limit?: number,
            exclusive?: boolean
        },
        callback?: (err?: null | Error, result?: {id: string, body: object}[]) => void
    ): Promise<{id: string, body: object}[]>;
    length(
        topic: string,
        callback?: (err?: null | Error, length?: number) => void
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
        },
        callback?: (err?: null | Error, result?: {id: string, body: object} | {id: string, body: object}[]) => void
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
        },
        callback?: (err?: Error | null, myProc?: IProc | IProc[]) => void
    ): Promise<IProc | IProc[]>;
    ack(
        procName: string,
        callback?: (err?: null | Error, logId?: string) => void
    ): Promise<string>;
    ackCommit(
        procName: string,
        commitLog: ICommitLog | ICommitLog[],
        callback?: (err?: null | Error, result?: {ackedLogId: string, id: string | string[]}) => void
    ): Promise<{ackedLogId: string, id: string | string[]}>;
    inspectProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc>;
    destroyProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc>;
    disableProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc>;
    resumeProc(
        procName: string,
        callback?: (err?: null | Error, myProc?: IProc) => void
    ): Promise<IProc>;
    reclaimProc(
        procName: string,
        callback?: (err?: null | Error, lastClaimedRange?: string) => void
    ): Promise<string>;
    liveProc(
        options: {
            topic: string,
            mode: "live" | "all",
            count?: number
        }
    ): ILiveProc;
}

export const pipeProcClient: IPipeProcClient = {
    messageMap: {},
    spawn: function(options, callback) {
        var cb = callback;
        var opts: {
            memory?: boolean,
            location?: string,
            workers?: number
        };
        if (typeof options === "function") { //only callback passed
            cb = options;
            opts = {};
        } else if (options && typeof options === "object") { //options passed
            opts = options;
        } else { //nothing passed
            opts = {};
        }
        return new Promise(function(resolve, reject) {
            spawn(pipeProcClient, {
                memory: opts.memory || false,
                location: opts.location || "./pipeproc_data",
                workers: (opts.workers && opts.workers >= 1 && opts.workers) || 0
            }, function(err, status) {
                if (err) {
                    reject(err);
                    if (typeof cb === "function") cb(err);
                } else {
                    resolve(status);
                    if (typeof cb === "function") cb(null, status);
                }
            });
        });
    },
    connect: function(options, callback) {
        var cb = callback;
        var opts: {
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
        return new Promise(function(resolve, reject) {
            connect(pipeProcClient, {
                isWorker: opts.isWorker || false
            }, function(err, status) {
                if (err) {
                    reject(err);
                    if (typeof cb === "function") cb(err);
                } else {
                    resolve(status);
                    if (typeof cb === "function") cb(null, status);
                }
            });
        });
    },
    shutdown: function(callback) {
        return new Promise(function(resolve, reject) {
            shutdown(pipeProcClient, function(err, status) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(status);
                    if (typeof callback === "function") callback(null, status);
                }
            });
        });
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
                    if (typeof cb === "function") cb(err);
                } else {
                    resolve(results);
                    if (typeof cb === "function") cb(null, results);
                }
            });
        });
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
                    if (typeof cb === "function") cb(err);
                } else {
                    resolve(results);
                    if (typeof cb === "function") cb(null, results);
                }
            });
        });
    },
    length: function(topic, callback) {
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
                    if (typeof callback === "function") callback(err);
                } else {
                    if (results && results[0]) {
                        const l = parseInt(results[0].id.split("-")[1]) + 1;
                        resolve(l);
                        if (typeof callback === "function") callback(null, l);
                    } else {
                        if (typeof callback === "function") callback(null, 0);
                        resolve(0);
                    }
                }
            });
        });
    },
    commit: function(commitLog, callback) {
        return new Promise(function(resolve, reject) {
            commit(pipeProcClient, commitLog, function(err, logId) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    if (err) {
                        reject(err);
                        if (typeof callback === "function") callback(err);
                    } else {
                        resolve(logId);
                        if (typeof callback === "function") callback(null, logId);
                    }
                }
            });
        });
    },
    proc: function(topic, options, callback) {
        return new Promise(function(resolve, reject) {
            proc(pipeProcClient, topic, options, function(err, result) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(result);
                    if (typeof callback === "function") callback(null, result);
                }
            });
        });
    },
    systemProc: function(options, callback) {
        return new Promise(function(resolve, reject) {
            systemProc(pipeProcClient, options, function(err, myProc) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(myProc);
                    if (typeof callback === "function") callback(null, myProc);
                }
            });
        });
    },
    ack: function(procName, callback) {
        return new Promise(function(resolve, reject) {
            ack(pipeProcClient, procName, function(err, logId) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(logId);
                    if (typeof callback === "function") callback(null, logId);
                }
            });
        });
    },
    ackCommit: function(procName, commitLog, callback) {
        return new Promise(function(resolve, reject) {
            ackCommit(pipeProcClient, procName, commitLog, function(err, results) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(results);
                    if (typeof callback === "function") callback(null, results);
                }
            });
        });
    },
    inspectProc: function(procName, callback) {
        return new Promise(function(resolve, reject) {
            inspectProc(pipeProcClient, procName, function(err, myProc) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(myProc);
                    if (typeof callback === "function") callback(null, myProc);
                }
            });
        });
    },
    destroyProc: function(procName, callback) {
        return new Promise(function(resolve, reject) {
            destroyProc(pipeProcClient, procName, function(err, myProc) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(myProc);
                    if (typeof callback === "function") callback(null, myProc);
                }
            });
        });
    },
    disableProc: function(procName, callback) {
        return new Promise(function(resolve, reject) {
            disableProc(pipeProcClient, procName, function(err, myProc) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(myProc);
                    if (typeof callback === "function") callback(null, myProc);
                }
            });
        });
    },
    resumeProc: function(procName, callback) {
        return new Promise(function(resolve, reject) {
            resumeProc(pipeProcClient, procName, function(err, myProc) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(myProc);
                    if (typeof callback === "function") callback(null, myProc);
                }
            });
        });
    },
    reclaimProc: function(procName, callback) {
        return new Promise(function(resolve, reject) {
            reclaimProc(pipeProcClient, procName, function(err, lastClaimedRange) {
                if (err) {
                    reject(err);
                    if (typeof callback === "function") callback(err);
                } else {
                    resolve(lastClaimedRange);
                    if (typeof callback === "function") callback(null, lastClaimedRange);
                }
            });
        });
    },
    liveProc: function(options) {
        return createLiveProc(pipeProcClient, options);
    }
};
