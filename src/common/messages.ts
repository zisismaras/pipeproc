import {v4 as uuid} from "uuid";
import {ICommitLog} from "../client";
import {IProc} from "../node/proc";
import {ISystemProc} from "../node/systemProc";

export interface IPipeProcMessageSchema {
    type: string;
    msgKey?: string;
    errStatus?: string;
    data?: object;
}

export interface IPipeProcMessage extends IPipeProcMessageSchema {
    msgKey: string;
}

export interface IPipeProcLogMessage extends IPipeProcMessage {
    data: {
        commitLog: {
            topic: string;
            body: string;
        } | {
            topic: string;
            body: string;
        }[]
    };
}

export interface IPipeProcLogMessageReply extends IPipeProcMessage {
    data: {
        id: string | string[];
    };
}

export interface IPipeProcAckLogMessage extends IPipeProcMessage {
    data: {
        procName: string;
        commitLog: {
            topic: string;
            body: string;
        } | {
            topic: string;
            body: string;
        }[]
    };
}

export interface IPipeProcAckLogMessageReply extends IPipeProcMessage {
    data: {
        ackedLogId: string;
        id: string | string[];
    };
}

export interface IPipeProcProcMessage extends IPipeProcMessage {
    data: {
        options: {
            name: string;
            topic: string;
            offset: string;
            count: number;
            maxReclaims: number;
            reclaimTimeout: number;
            onMaxReclaimsReached: string;
        }
    };
}

export interface IPipeProcProcMessageReply extends IPipeProcMessage {
    data?: {
        id: string;
        data: string
    } | {
        id: string;
        data: string;
    }[];
}

export interface IPipeProcSystemProcMessage extends IPipeProcMessage {
    data: {
        options: {
            name: string;
            offset: string;
            count: number;
            maxReclaims: number;
            reclaimTimeout: number;
            onMaxReclaimsReached: string;
            from: string | string[];
            to: string | string[];
            inlineProcessor?: string;
            externalProcessor?: string;
        }
    };
    type: "system_proc";
}

export interface IPipeProcSystemProcMessageReply extends IPipeProcMessage {
    data: {
        proc?: IProc | IProc[];
    };
    type: "system_proc_ok" | "system_proc_error";
    errStatus?: string;
}

export interface IPipeProcRangeMessage extends IPipeProcMessage {
    data: {
        topic: string,
        options: {
            start: string,
            end: string,
            limit: number,
            exclusive: boolean,
            reverse: false
        }
    };
}

export interface IPipeProcRangeMessageReply extends IPipeProcMessage {
    data: {
        results: {id: string, data: string}[];
    };
}

export interface IPipeProcAckMessage extends IPipeProcMessage {
    data: {
        procName: string;
    };
}
export interface IPipeProcAckMessageReply extends IPipeProcMessage {
    data: {
        id: string;
    };
}
export interface IPipeProcInspectProcMessage extends IPipeProcMessage {
    data: {
        procName: string;
    };
}
export interface IPipeProcInspectProcMessageReply extends IPipeProcMessage {
    data: {
        proc: IProc;
    };
}
export interface IPipeProcDisableProcMessage extends IPipeProcMessage {
    data: {
        procName: string;
    };
}
export interface IPipeProcDisableProcMessageReply extends IPipeProcMessage {
    data: {
        proc: IProc;
    };
}
export interface IPipeProcResumeProcMessage extends IPipeProcMessage {
    data: {
        procName: string;
    };
}
export interface IPipeProcResumeProcMessageReply extends IPipeProcMessage {
    data: {
        proc: IProc;
    };
}
export interface IPipeProcDestroyProcMessage extends IPipeProcMessage {
    data: {
        procName: string;
    };
}
export interface IPipeProcDestroyProcMessageReply extends IPipeProcMessage {
    data: {
        proc: IProc;
    };
}
export interface IPipeProcReclaimProcMessage extends IPipeProcMessage {
    data: {
        procName: string;
    };
}
export interface IPipeProcReclaimProcMessageReply extends IPipeProcMessage {
    data: {
        lastClaimedRange: string;
    };
}

export interface IPipeProcSystemInitMessage extends IPipeProcMessage {
    data: {
        options: {
            memory?: boolean,
            location?: string,
            workers?: number,
            gc?: {
                minPruneTime?: number,
                interval?: number
            } | boolean
        }
    };
}

export interface IPipeProcWorkerInitMessage extends IPipeProcMessage {
    type: "worker_init";
    data: {
        address: string;
        tls: {
            key: string;
            cert: string;
            ca: string;
        } | false
    };
}

export interface IPipeProcInitIPCMessage extends IPipeProcMessage {
    type: "init_ipc";
    data: {
        address: string;
        tls: {
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
        } | false
    };
}

export interface IPipeProcIPCEstablishedMessage extends IPipeProcMessage {
    type: "ipc_established";
}

export interface IPipeProcWorkerInitMessageReply extends IPipeProcMessage {
    type: "connected" | "connection_failure";
    errStatus?: string;
}

export interface IPipeProcRegisterSystemProcsMessage extends IPipeProcMessage {
    type: "register_system_procs";
    data: {
        procs: IProc[];
        systemProcs: ISystemProc[];
    };
}

export interface IPipeProcWaitForProcsMessage extends IPipeProcMessage {
    type: "wait_for_procs";
    data: {
        procs: string[];
    };
}

export interface IPipeProcRegisterSystemProcsMessageReply extends IPipeProcMessage {
    type: "register_system_proc_ok" | "register_system_proc_error";
    errStatus?: string;
}

export function prepareMessage(msg: IPipeProcMessageSchema): IPipeProcMessage {
    if (!msg.msgKey) {
        msg.msgKey = uuid();
    }
    return {type: msg.type, msgKey: msg.msgKey, data: msg.data, errStatus: msg.errStatus};
}

export function prepareLogMessage(msg: IPipeProcMessage, commitLog: ICommitLog | ICommitLog[]): IPipeProcLogMessage {
    if (Array.isArray(commitLog)) {
        return {
            type: msg.type,
            msgKey: msg.msgKey,
            data: {
                commitLog: commitLog.map(l => {
                    return {topic: l.topic, body: JSON.stringify(l.body)};
                })
            }
        };
    } else {
        return {
            type: msg.type,
            msgKey: msg.msgKey,
            data: {
                commitLog: {
                    topic: commitLog.topic,
                    body: JSON.stringify(commitLog.body)
                }
            }
        };
    }
}

export function prepareProcMessage(
    msg: IPipeProcMessage,
    options: {
        name: string,
        topic: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string
    }
): IPipeProcProcMessage {
    if (!msg.msgKey) {
        msg.msgKey = uuid();
    }
    return {
        type: msg.type,
        msgKey: msg.msgKey,
        data: {
            options: {
                name: options.name,
                topic: options.topic,
                offset: options.offset,
                count: options.count,
                maxReclaims: options.maxReclaims,
                reclaimTimeout: options.reclaimTimeout,
                onMaxReclaimsReached: options.onMaxReclaimsReached
            }
        }
    };
}

export function prepareSystemProcMessage(
    options: {
        name: string,
        offset: string,
        count: number,
        maxReclaims: number,
        reclaimTimeout: number,
        onMaxReclaimsReached: string,
        from: string | string[],
        to: string | string[],
        inlineProcessor?: string,
        externalProcessor?: string
    }
): IPipeProcSystemProcMessage {
    return {
        type: "system_proc",
        msgKey: uuid(),
        data: {
            options: {
                name: options.name,
                offset: options.offset,
                count: options.count,
                maxReclaims: options.maxReclaims,
                reclaimTimeout: options.reclaimTimeout,
                onMaxReclaimsReached: options.onMaxReclaimsReached,
                from: options.from,
                to: options.to,
                inlineProcessor: options.inlineProcessor,
                externalProcessor: options.externalProcessor
            }
        }
    };
}

export function prepareAckLogMessage(
    msg: IPipeProcMessage,
    procname: string,
    commitLog: ICommitLog | ICommitLog[]
): IPipeProcAckLogMessage {
    if (Array.isArray(commitLog)) {
        return {
            type: msg.type,
            msgKey: msg.msgKey,
            data: {
                procName: procname,
                commitLog: commitLog.map(l => {
                    return {topic: l.topic, body: JSON.stringify(l.body)};
                })
            }
        };
    } else {
        return {
            type: msg.type,
            msgKey: msg.msgKey,
            data: {
                procName: procname,
                commitLog: {
                    topic: commitLog.topic,
                    body: JSON.stringify(commitLog.body)
                }
            }
        };
    }
}

export function prepareAckMessage(
    msg: IPipeProcMessage,
    procname: string
): IPipeProcAckMessage {
    return {
        type: msg.type,
        msgKey: msg.msgKey,
        data: {
            procName: procname
        }
    };
}

export function prepareDestroyProcMessage(
    msg: IPipeProcMessage,
    procname: string
): IPipeProcDestroyProcMessage {
    return {
        type: msg.type,
        msgKey: msg.msgKey,
        data: {
            procName: procname
        }
    };
}

export function prepareDisableProcMessage(
    msg: IPipeProcMessage,
    procname: string
): IPipeProcDisableProcMessage {
    return {
        type: msg.type,
        msgKey: msg.msgKey,
        data: {
            procName: procname
        }
    };
}

export function prepareResumeProcMessage(
    msg: IPipeProcMessage,
    procname: string
): IPipeProcResumeProcMessage {
    return {
        type: msg.type,
        msgKey: msg.msgKey,
        data: {
            procName: procname
        }
    };
}

export function prepareReclaimProcMessage(
    msg: IPipeProcMessage,
    procname: string
): IPipeProcReclaimProcMessage {
    return {
        type: msg.type,
        msgKey: msg.msgKey,
        data: {
            procName: procname
        }
    };
}

export function prepareWorkerInitMessage(
    address: string,
    tls: {
        key: string;
        cert: string;
        ca: string;
    } | false
): IPipeProcWorkerInitMessage {
    return {
        type: "worker_init",
        msgKey: uuid(),
        data: {
            address: address,
            tls: tls
        }
    };
}

export function prepareRegisterSystemProcsMessage(
    procs: IProc[],
    systemProcs: ISystemProc[]
): IPipeProcRegisterSystemProcsMessage {
    return {
        type: "register_system_procs",
        msgKey: uuid(),
        data: {
            procs: procs,
            systemProcs: systemProcs
        }
    };
}
