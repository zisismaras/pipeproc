import debug from "debug";
import {isAbsolute, resolve} from "path";
import LevelDOWN from "leveldown";
import MemDOWN from "memdown";
import {
    IPipeProcMessage,
    IPipeProcInitIPCMessage,

    IPipeProcLogMessage,
    IPipeProcProcMessage,
    IPipeProcRangeMessage,
    IPipeProcSystemInitMessage,
    IPipeProcAckMessage,
    IPipeProcAckLogMessage,
    IPipeProcInspectProcMessage,
    IPipeProcDestroyProcMessage,
    IPipeProcDisableProcMessage,
    IPipeProcResumeProcMessage,
    IPipeProcReclaimProcMessage,
    IPipeProcSystemProcMessage,
    IPipeProcWaitForProcsMessage,
    IPipeProcPingMessage,

    IPipeProcLogMessageReply,
    IPipeProcProcMessageReply,
    IPipeProcRangeMessageReply,
    IPipeProcAckMessageReply,
    IPipeProcAckLogMessageReply,
    IPipeProcInspectProcMessageReply,
    IPipeProcDestroyProcMessageReply,
    IPipeProcDisableProcMessageReply,
    IPipeProcResumeProcMessageReply,
    IPipeProcReclaimProcMessageReply,
    IPipeProcSystemProcMessageReply,
    IPipeProcPingMessageReply,
    prepareMessage
} from "../common/messages";
import {commitLog} from "./commitLog";
import {restoreState} from "./restoreState";
import {runShutdownHooks} from "./shutdown";
import {getRange} from "./getRange";
import {proc, IProc} from "./proc";
import {systemProc, ISystemProc} from "./systemProc";
import {IWorker, spawnWorkers} from "./workerManager";
import {ack} from "./ack";
import {ackCommitLog} from "./ackCommitLog";
import {destroyProc} from "./destroyProc";
import {initializeMessages, registerMessage, IMessageRegistry} from "./messaging";
import {IWriteBuffer, startWriteBuffer} from "./writeBuffer";
import {disableProc, resumeProc} from "./resumeDisableProc";
import {reclaimProc} from "./reclaimProc";
import {collect} from "./gc/collect";
import {waitForProcs} from "./waitForProcs";
import {ServerSocket} from "../socket/bind";

const d = debug("pipeproc:node");

let db: LevelDOWN.LevelDown;
let connectionAddress: string;
let serverSocket: ServerSocket;
let clientTLS: {
    key: string;
    cert: string;
    ca: string;
} | false;

export interface IActiveTopics {
    [key: string]: {
        currentTone: number,
        createdAt: number
    };
}

export interface ISystemState {
    active: boolean;
}
const activeTopics: IActiveTopics = {};

const systemState: ISystemState = {active: false};

const messageRegistry: IMessageRegistry = {};

const writeBuffer: IWriteBuffer = [];

const activeProcs: IProc[] = [];

const activeSystemProcs: ISystemProc[] = [];

const activeWorkers: IWorker[] = [];

registerMessage<IPipeProcSystemInitMessage["data"], IPipeProcMessage["data"]>(messageRegistry, {
    messageType: "system_init",
    replySuccess: "system_ready",
    replyError: "system_ready_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        if (systemState.active) {
            return callback("system_already_active");
        }
        d("starting up...");
        if (data.options.memory) {
            d("using in-memory adapter");
            db = MemDOWN();
        } else {
            d("using disk adapter");
            if (data.options.location) {
                let location: string;
                if (isAbsolute(data.options.location)) {
                    location = data.options.location;
                } else {
                    location = resolve(data.options.location);
                }
                d("data location:", location);
                db = LevelDOWN(location);
            } else {
                db = LevelDOWN("./pipeproc");
            }
        }
        restoreState(
            db,
            activeTopics,
            systemState,
            activeProcs,
            activeSystemProcs,
            data.options.memory,
        function(err) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                spawnWorkers(
                    data.options.workers || 0,
                    activeWorkers, activeProcs, activeSystemProcs, connectionAddress, clientTLS,
                function(spawnErr) {
                    if (err) {
                        callback((spawnErr && spawnErr.message) || "uknown_error");
                    } else {
                        if (data.options.gc) {
                            d("gc is enabled");
                            const MIN_PRUNE_TIME = (data.options.gc &&
                                (<{minPruneTime: number}>data.options.gc).minPruneTime) || 30000;
                            const GC_INTERVAL = (data.options.gc &&
                                (<{interval: number}>data.options.gc).interval) || 30000;
                            let gcRunning = false;
                            setInterval(function() {
                                if (gcRunning) return;
                                d("gc will start running...");
                                gcRunning = true;
                                collect(db, activeTopics, activeProcs, {minPruneTime: MIN_PRUNE_TIME}, function(gcErr) {
                                    if (gcErr) {
                                        d("gc error:", gcErr);
                                    }
                                    gcRunning = false;
                                    d("gc ended");
                                });
                            }, GC_INTERVAL);
                        } else {
                            d("gc is disabled");
                        }
                        callback();
                    }
                });
            }
        });
    }
});

registerMessage<IPipeProcPingMessage["data"], IPipeProcPingMessageReply["data"]>(messageRegistry, {
    messageType: "ping",
    replySuccess: "pong",
    replyError: "ping_error",
    writeOp: false,
    listener: function(
        _data,
        callback
    ) {
        callback();
    }
});

registerMessage<IPipeProcWaitForProcsMessage["data"], IPipeProcMessage["data"]>(messageRegistry, {
    messageType: "wait_for_procs",
    replySuccess: "procs_completed",
    replyError: "procs_completed_error",
    writeOp: false,
    listener: function(
        data,
        callback
    ) {
        d("waiting for procs...");
        waitForProcs(activeTopics, activeProcs, data.procs, function() {
            callback();
        });
    }
});

registerMessage<IPipeProcLogMessage["data"], IPipeProcLogMessageReply["data"]>(messageRegistry, {
    messageType: "commit",
    replySuccess: "commit_completed",
    replyError: "commit_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        commitLog(db, activeTopics, data.commitLog, function(err, id) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (id) {
                    callback(null, {id: id});
                } else {
                    callback("uncommited");
                }
            }
        });
    }
});

registerMessage<IPipeProcRangeMessage["data"], IPipeProcRangeMessageReply["data"]>(messageRegistry, {
    messageType: "get_range",
    replySuccess: "range_reply",
    replyError: "range_error",
    writeOp: false,
    listener: function(
        data,
        callback
    ) {
        getRange(
            db,
            activeTopics,
            data.topic,
            data.options.start,
            data.options.end,
            data.options.limit,
            data.options.exclusive,
            data.options.reverse,
        function(err, results) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (results) {
                    callback(null, {results});
                } else {
                    callback();
                }
            }
        });
    }
});

registerMessage<IPipeProcProcMessage["data"], IPipeProcProcMessageReply["data"]>(messageRegistry, {
    messageType: "proc",
    replySuccess: "proc_ok",
    replyError: "proc_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        proc(db, activeProcs, activeTopics, data.options, function(err, log) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (log) {
                    callback(null, log);
                } else {
                    callback();
                }
            }
        });
    }
});

registerMessage<IPipeProcSystemProcMessage["data"], IPipeProcSystemProcMessageReply["data"]>(messageRegistry, {
    messageType: "system_proc",
    replySuccess: "system_proc_ok",
    replyError: "system_proc_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        systemProc(db, activeProcs, activeSystemProcs, activeWorkers, data.options, function(err, myProc) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                callback(null, {proc: myProc});
            }
        });
    }
});

registerMessage<IPipeProcAckMessage["data"], IPipeProcAckMessageReply["data"]>(messageRegistry, {
    messageType: "ack",
    replySuccess: "ack_ok",
    replyError: "ack_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        ack(db, activeProcs, data.procName, function(err, ackedLogId) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (ackedLogId) {
                    callback(null, {id: ackedLogId});
                } else {
                    callback("invalid_ack");
                }
            }
        });
    }
});

registerMessage<IPipeProcAckLogMessage["data"], IPipeProcAckLogMessageReply["data"]>(messageRegistry, {
    messageType: "ack_commit",
    replySuccess: "ack_commit_completed",
    replyError: "ack_commit_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        ackCommitLog(db, activeTopics, activeProcs, data.procName, data.commitLog, function(err, status) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                const ackedLogId = status[0];
                const commitedId = status[1];
                if (status && ackedLogId && commitedId) {
                    callback(null, {ackedLogId: ackedLogId, id: commitedId});
                } else {
                    callback("invalid_ack_or_commit");
                }
            }
        });
    }
});

registerMessage<IPipeProcInspectProcMessage["data"], IPipeProcInspectProcMessageReply["data"]>(messageRegistry, {
    messageType: "inspect_proc",
    replySuccess: "inspect_proc_reply",
    replyError: "inspect_proc_error",
    writeOp: false,
    listener: function(
        data,
        callback
    ) {
        const myProc = activeProcs.find(p => p.name === data.procName);
        d("proc inspection request:", data.procName);
        if (myProc) {
            callback(null, {proc: myProc});
        } else {
            callback("invalid_proc");
        }
    }
});

registerMessage<IPipeProcDestroyProcMessage["data"], IPipeProcDestroyProcMessageReply["data"]>(messageRegistry, {
    messageType: "destroy_proc",
    replySuccess: "destroy_proc_ok",
    replyError: "destroy_proc_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        destroyProc(db, activeProcs, data.procName, function(err, myProc) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (myProc) {
                    callback(null, {proc: myProc});
                } else {
                    callback("invalid_proc");
                }
            }
        });
    }
});

registerMessage<IPipeProcDisableProcMessage["data"], IPipeProcDisableProcMessageReply["data"]>(messageRegistry, {
    messageType: "disable_proc",
    replySuccess: "disable_proc_ok",
    replyError: "disable_proc_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        disableProc(db, activeProcs, data.procName, function(err, myProc) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (myProc) {
                    callback(null, {proc: myProc});
                } else {
                    callback("invalid_proc");
                }
            }
        });
    }
});

registerMessage<IPipeProcResumeProcMessage["data"], IPipeProcResumeProcMessageReply["data"]>(messageRegistry, {
    messageType: "resume_proc",
    replySuccess: "resume_proc_ok",
    replyError: "resume_proc_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        resumeProc(db, activeProcs, data.procName, function(err, myProc) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                if (myProc) {
                    callback(null, {proc: myProc});
                } else {
                    callback("invalid_proc");
                }
            }
        });
    }
});

registerMessage<IPipeProcReclaimProcMessage["data"], IPipeProcReclaimProcMessageReply["data"]>(messageRegistry, {
    messageType: "reclaim_proc",
    replySuccess: "reclaim_proc_ok",
    replyError: "reclaim_proc_error",
    writeOp: true,
    listener: function(
        data,
        callback
    ) {
        reclaimProc(db, activeProcs, data.procName, function(err, lastClaimedRange) {
            if (err) {
                callback((err && err.message) || "uknown_error");
            } else {
                callback(null, {lastClaimedRange: lastClaimedRange || ""});
            }
        });
    }
});

const initIPCListener = function(e: IPipeProcInitIPCMessage) {
    if (e.type === "init_ipc") {
        process.removeListener("message", initIPCListener);
        connectionAddress = e.data.address;
        clientTLS = e.data.tls && e.data.tls.client;
        initializeMessages(writeBuffer, messageRegistry, connectionAddress, e.data.tls && e.data.tls.server, function(err, socket) {
            if (err) {
                if (process && typeof process.send === "function") {
                    process.send(prepareMessage({
                        type: "ipc_established",
                        msgKey: e.msgKey,
                        errStatus: err.message
                    }));
                }
            } else {
                serverSocket = <ServerSocket>socket;
                if (process && typeof process.send === "function") {
                    process.send(prepareMessage({type: "ipc_established", msgKey: e.msgKey}));
                }
            }
        });
    }
};

const shutdownListener = function(e: IPipeProcMessage) {
    if (e.type === "system_shutdown") {
        d("shutting down...");
        serverSocket.close();
        process.removeListener("message", shutdownListener);
        runShutdownHooks(db, systemState, activeWorkers, function(err) {
            if (err) {
                if (process && typeof process.send === "function") {
                    process.send(prepareMessage({
                        type: "system_closed_error",
                        msgKey: e.msgKey,
                        errStatus: (err && err.message) || "uknown_error"
                    }));
                }
                process.exit(1);
            } else {
                if (process && typeof process.send === "function") {
                    process.send(prepareMessage({
                        type: "system_closed",
                        msgKey: e.msgKey
                    }));
                }
                process.exit(0);
            }
        });
    }
};

process.on("message", initIPCListener);
process.on("message", shutdownListener);
startWriteBuffer(writeBuffer);
