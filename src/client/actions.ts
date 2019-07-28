import {
    prepareReclaimProcMessage,
    prepareDisableProcMessage,
    prepareResumeProcMessage,
    prepareDestroyProcMessage,
    prepareAckLogMessage,
    prepareAckMessage,
    prepareLogMessage,
    prepareProcMessage,
    prepareSystemProcMessage,
    prepareAvailableProcMessage,
    prepareMessage,
    IPipeProcLogMessageReply,
    IPipeProcRangeMessageReply,
    IPipeProcAckLogMessageReply,
    IPipeProcInspectProcMessageReply,
    IPipeProcDestroyProcMessageReply,
    IPipeProcDisableProcMessageReply,
    IPipeProcResumeProcMessageReply,
    IPipeProcReclaimProcMessageReply,
    IPipeProcAckMessageReply,
    IPipeProcProcMessageReply,
    IPipeProcSystemProcMessageReply,
    IPipeProcAvailableProcMessageReply
} from "../common/messages";
import {IPipeProcClient, ICommitLog} from ".";
import {sendMessageToNode} from "./process";
import {resolve} from "path";

export function commit(
    client: IPipeProcClient,
    commitLog: ICommitLog | ICommitLog[],
    callback: (err?: Error | null, logId?: IPipeProcLogMessageReply["data"]["id"]) => void
): void {
    if (!areValidTopics(commitLog)) {
        return callback(new Error("invalid_topic_format"));
    }
    if (!areValidLogs(commitLog)) {
        return callback(new Error("invalid_log_format"));
    }
    if (client.pipeProcNode) {
        const msg = prepareLogMessage(prepareMessage({type: "commit"}), commitLog);
        client.messageMap[msg.msgKey] = function(e: IPipeProcLogMessageReply) {
            if (e.type === "commit_completed") {
                if (e.data && e.data.id) {
                    callback(null, e.data.id);
                } else {
                    callback(null, "");
                }
            } else if (e.type === "commit_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export type innerRange = (
    client: IPipeProcClient,
    topic: string,
    options: {
        start: string,
        end: string,
        limit: number,
        exclusive: boolean,
        reverse: boolean
    },
    callback: (err?: null | Error, result?: {id: string, body: object}[]) => void
) => void;

export const range: innerRange = function(client, topic, options, callback) {
    if (!isValidTopic(topic)) {
        return callback(new Error("invalid_topic_format"));
    }
    if (client.pipeProcNode) {
        const msg = prepareMessage({type: "get_range", data: {topic: topic, options: options}});
        client.messageMap[msg.msgKey] = function(e: IPipeProcRangeMessageReply): void {
            if (e.msgKey === msg.msgKey) {
                if (e.type === "range_reply") {
                    try {
                        const results = e.data.results.map(re => {
                            return {id: re.id, body: JSON.parse(re.data)};
                        });
                        callback(null, results);
                    } catch (e) {
                        callback(new Error("malformed_log"));
                    }
                } else if (e.type === "range_error") {
                    callback(new Error(e.errStatus));
                }
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
};

export function ack(
    client: IPipeProcClient,
    procName: string,
    callback: (err?: Error | null, logId?: string) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareAckMessage(prepareMessage({type: "ack"}), procName);
        client.messageMap[msg.msgKey] = function(e: IPipeProcAckMessageReply) {
            if (e.type === "ack_ok") {
                callback(null, e.data.id);
            } else if (e.type === "ack_error") {
                callback(new Error(e.errStatus));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function ackCommit(
    client: IPipeProcClient,
    procName: string,
    commitLog: ICommitLog | ICommitLog[],
    callback: (err?: Error | null, result?: {ackedLogId: string, id: string | string[]}) => void
): void {
    if (!areValidTopics(commitLog)) {
        return callback(new Error("invalid_topic_format"));
    }
    if (!areValidLogs(commitLog)) {
        return callback(new Error("invalid_log_format"));
    }
    if (client.pipeProcNode) {
        const msg = prepareAckLogMessage(prepareMessage({type: "ack_commit"}), procName, commitLog);
        client.messageMap[msg.msgKey] = function(e: IPipeProcAckLogMessageReply) {
            if (e.type === "ack_commit_completed") {
                callback(null, {ackedLogId: e.data.ackedLogId, id: e.data.id});
            } else if (e.type === "ack_commit_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function proc(
    client: IPipeProcClient,
    topic: string,
    options: {
        name: string,
        offset: string,
        count?: number,
        maxReclaims?: number,
        reclaimTimeout?: number,
        onMaxReclaimsReached?: string
    },
    callback: (err?: null | Error, result?: {id: string, body: object} | {id: string, body: object}[]) => void
): void {
    if (client.pipeProcNode) {
        if (options.onMaxReclaimsReached !== "disable" && options.onMaxReclaimsReached !== "continue") {
            options.onMaxReclaimsReached = "disable";
        }
        const msg = prepareProcMessage(prepareMessage({type: "proc"}), {
            topic: topic,
            name: options.name,
            offset: options.offset,
            count: options.count || 1,
            maxReclaims: options.maxReclaims || 10,
            reclaimTimeout: options.reclaimTimeout || 10000,
            onMaxReclaimsReached: options.onMaxReclaimsReached
        });
        client.messageMap[msg.msgKey] = function(e: IPipeProcProcMessageReply) {
            if (e.type === "proc_ok") {
                if (e.data) {
                    if (Array.isArray(e.data)) {
                        try {
                            const results = e.data.map(re => {
                                return {id: re.id, body: JSON.parse(re.data)};
                            });
                            callback(null, results);
                        } catch (e) {
                            callback(new Error("malformed_log"));
                        }
                    } else {
                        try {
                            const result = {id: e.data.id, body: JSON.parse(e.data.data)};
                            callback(null, result);
                        } catch (e) {
                            callback(new Error("malformed_log"));
                        }
                    }
                } else {
                    callback();
                }
            } else if (e.type === "proc_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function availableProc(
    client: IPipeProcClient,
    procList: {
        topic: string,
        name: string,
        offset: string,
        count?: number,
        maxReclaims?: number,
        reclaimTimeout?: number,
        onMaxReclaimsReached?: string
    }[],
    callback: (err?: null | Error, result?: {
        procName?: string,
        log?: {id: string, body: object} | {id: string, body: object}[]
    }) => void
): void {
    if (!client.pipeProcNode) return callback(new Error("no_active_node"));
    //TODO: validate topic, name, offset
    const procListWithDefaults = procList.map(function(pr) {
        return {
            topic: pr.topic,
            name: pr.name,
            offset: pr.offset,
            count: pr.count || 1,
            maxReclaims: pr.maxReclaims || 10,
            reclaimTimeout: pr.reclaimTimeout || 10000,
            onMaxReclaimsReached: pr.onMaxReclaimsReached !== "disable" &&
                                    pr.onMaxReclaimsReached !== "continue" ? "disable" : pr.onMaxReclaimsReached
        };
    });
    const msg = prepareAvailableProcMessage(prepareMessage({type: "available_proc"}), procListWithDefaults);
    client.messageMap[msg.msgKey] = function(e: IPipeProcAvailableProcMessageReply) {
        if (e.type === "available_proc_ok") {
            if (!e.data || !e.data.log) return callback();
            if (Array.isArray(e.data.log)) {
                try {
                    const results = e.data.log.map(re => {
                        return {id: re.id, body: JSON.parse(re.data)};
                    });
                    callback(null, {procName: e.data.procName, log: results});
                } catch (e) {
                    callback(new Error("malformed_log"));
                }
            } else {
                try {
                    const result = {id: e.data.log.id, body: JSON.parse(e.data.log.data)};
                    callback(null, {procName: e.data.procName, log: result});
                } catch (e) {
                    callback(new Error("malformed_log"));
                }
            }
        } else {
            callback(new Error(e.errStatus || "uknown_error"));
        }
    };
    sendMessageToNode(client, msg);
}

export function systemProc(
    client: IPipeProcClient,
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
    callback: (err?: Error | null, myProc?: IPipeProcSystemProcMessageReply["data"]["proc"]) => void
): void {
    if (client.pipeProcNode) {
        if (options.onMaxReclaimsReached !== "disable" && options.onMaxReclaimsReached !== "continue") {
            options.onMaxReclaimsReached = "disable";
        }
        let inlineProcessor;
        let externalProcessor;
        if (typeof options.processor === "function") {
            inlineProcessor = options.processor.toString();
        } else {
            externalProcessor = resolve(options.processor);
        }
        const msg = prepareSystemProcMessage({
            name: options.name,
            offset: options.offset,
            count: options.count || 1,
            maxReclaims: options.maxReclaims || 10,
            reclaimTimeout: options.reclaimTimeout || 10000,
            onMaxReclaimsReached: options.onMaxReclaimsReached,
            from: options.from,
            to: options.to || "",
            inlineProcessor: inlineProcessor,
            externalProcessor: externalProcessor
        });
        if (client.pipeProcNode) {
            client.messageMap[msg.msgKey] = function(e: IPipeProcSystemProcMessageReply): void {
                if (e.type === "system_proc_ok") {
                    callback(null, e.data.proc);
                } else if (e.type === "system_proc_error") {
                    callback(new Error(e.errStatus));
                }
            };
            sendMessageToNode(client, msg);
        } else {
            callback(new Error("no_active_node"));
        }
    }
}

export function inspectProc(
    client: IPipeProcClient,
    procName: string,
    callback: (err?: Error | null, myProc?: IPipeProcInspectProcMessageReply["data"]["proc"]) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareMessage({type: "inspect_proc", data: {procName: procName}});
        client.messageMap[msg.msgKey] = function(e: IPipeProcInspectProcMessageReply): void {
            if (e.type === "inspect_proc_reply") {
                callback(null, e.data.proc);
            } else if (e.type === "inspect_proc_error") {
                callback(new Error(e.errStatus));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function destroyProc(
    client: IPipeProcClient,
    procName: string,
    callback: (err?: Error | null, myProc?: IPipeProcInspectProcMessageReply["data"]["proc"]) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareDestroyProcMessage(prepareMessage({type: "destroy_proc"}), procName);
        client.messageMap[msg.msgKey] = function(e: IPipeProcDestroyProcMessageReply) {
            if (e.type === "destroy_proc_ok") {
                callback(null, e.data.proc);
            } else if (e.type === "destroy_proc_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function disableProc(
    client: IPipeProcClient,
    procName: string,
    callback: (err?: Error | null, myProc?: IPipeProcInspectProcMessageReply["data"]["proc"]) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareDisableProcMessage(prepareMessage({type: "disable_proc"}), procName);
        client.messageMap[msg.msgKey] = function(e: IPipeProcDisableProcMessageReply) {
            if (e.type === "disable_proc_ok") {
                callback(null, e.data.proc);
            } else if (e.type === "disable_proc_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function resumeProc(
    client: IPipeProcClient,
    procName: string,
    callback: (err?: Error | null, myProc?: IPipeProcInspectProcMessageReply["data"]["proc"]) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareResumeProcMessage(prepareMessage({type: "resume_proc"}), procName);
        client.messageMap[msg.msgKey] = function(e: IPipeProcResumeProcMessageReply) {
            if (e.type === "resume_proc_ok") {
                callback(null, e.data.proc);
            } else if (e.type === "resume_proc_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function reclaimProc(
    client: IPipeProcClient,
    procName: string,
    callback: (
        err?: Error | null,
        lastClaimedRange?: IPipeProcReclaimProcMessageReply["data"]["lastClaimedRange"]
    ) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareReclaimProcMessage(prepareMessage({type: "reclaim_proc"}), procName);
        client.messageMap[msg.msgKey] = function(e: IPipeProcReclaimProcMessageReply) {
            if (e.type === "reclaim_proc_ok") {
                callback(null, e.data.lastClaimedRange);
            } else if (e.type === "reclaim_proc_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

export function waitForProcs(
    client: IPipeProcClient,
    procFilter: string[],
    callback: (
        err?: Error | null
    ) => void
): void {
    if (client.pipeProcNode) {
        const msg = prepareMessage({type: "wait_for_procs", data: {procs: procFilter}});
        client.messageMap[msg.msgKey] = function() {
            callback();
        };
        sendMessageToNode(client, msg);
    } else {
        callback(new Error("no_active_node"));
    }
}

function areValidTopics(commitLog: ICommitLog | ICommitLog[]): boolean {
    if (Array.isArray(commitLog)) {
        return commitLog.filter(l => {
            return isValidTopic(l.topic);
        }).length === commitLog.length;
    } else {
        return isValidTopic(commitLog.topic);
    }
}

function isValidTopic(topic: string): boolean {
    if (topic && typeof topic === "string" && topic.match(/^[a-zA-Z0-9_-]+$/)) {
        return true;
    } else {
        return false;
    }
}

function areValidLogs(commitLog: ICommitLog | ICommitLog[]): boolean {
    if (Array.isArray(commitLog)) {
        return commitLog.filter(l => {
            return isValidLog(l.body);
        }).length === commitLog.length;
    } else {
        return isValidLog(commitLog.body);
    }
}

function isValidLog(log: object): boolean {
    if (log && typeof log === "object") {
        return true;
    } else {
        return false;
    }
}
