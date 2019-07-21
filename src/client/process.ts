import {fork as forkProcess, ChildProcess} from "child_process";
import debug from "debug";
import {prepareMessage, IPipeProcMessage, IPipeProcIPCEstablishedMessage} from "../common/messages";
import {IPipeProcClient} from ".";
import {tmpdir} from "os";

import {connect as socketConnect} from "../socket/connect";

const d = debug("pipeproc:client");

export function spawn(
    client: IPipeProcClient,
    options: {
        memory: boolean,
        location: string,
        workers: number,
        gc?: {minPruneTime?: number, interval?: number} | boolean
    },
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) return callback(null, "node_already_active");
    d("spawning node...");
    client.pipeProcNode = forkProcess(`${__dirname}/../node/pipeProc`);
    const initIPCMessage = prepareMessage({type: "init_ipc", data: {namespace: client.namespace}});
    const ipcEstablishedListener = function(e: IPipeProcIPCEstablishedMessage) {
        if ((e.type !== "ipc_established") || e.msgKey !== initIPCMessage.msgKey) return;
        if (e.errStatus) {
            return callback(new Error(e.errStatus));
        }
        d("ipc established under namespace:", client.namespace);
        (<ChildProcess>client.pipeProcNode).removeListener("message", ipcEstablishedListener);
        socketConnect(`ipc://${tmpdir()}/pipeproc.${client.namespace || "default"}`, {}, function(err, connectSocket) {
            if (err) {
                return callback(err);
            }
            client.connectSocket = connectSocket;
            //messageMap listener init
            client.connectSocket.onMessage<IPipeProcMessage>(function(message) {
                if (typeof client.messageMap[message.msgKey] === "function") {
                    client.messageMap[message.msgKey](message);
                }
                delete client.messageMap[message.msgKey];
            });
            const ipcd = debug("pipeproc:ipc:client");
            client.connectSocket.onError(function(ipcError) {
                ipcd("client IPC error:", ipcError);
            });
            client.connectSocket.send(prepareMessage({type: "system_init", data: {options: options}}), function() {
                callback(null, "spawned_and_connected");
            });
        });
    };
    (<ChildProcess>client.pipeProcNode).on("message", ipcEstablishedListener);
    (<ChildProcess>client.pipeProcNode).send(initIPCMessage);
}

export function connect(
    client: IPipeProcClient,
    options: {isWorker: boolean},
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.connectSocket) return callback(null, "already_connected");
    if (!client.pipeProcNode && options.isWorker) {
        client.pipeProcNode = process;
    } else if (!client.pipeProcNode) {
        client.pipeProcNode = {};
    }
    socketConnect(`ipc://${tmpdir()}/pipeproc.${client.namespace || "default"}`, {}, function(err, connectSocket) {
        if (err) {
            return callback(err);
        }
        d("connected to ipc channel");
        client.connectSocket = connectSocket;
        //messageMap listener init
        client.connectSocket.onMessage<IPipeProcMessage>(function(message) {
            if (typeof client.messageMap[message.msgKey] === "function") {
                client.messageMap[message.msgKey](message);
            }
            delete client.messageMap[message.msgKey];
        });
        if (options.isWorker) {
            const ipcd = debug("pipeproc:ipc:worker");
            client.connectSocket.onError(function(ipcErr) {
                ipcd("client IPC error:", ipcErr);
            });
        } else {
            const ipcd = debug("pipeproc:ipc:client");
            client.connectSocket.onError(function(ipcErr) {
                ipcd("client IPC error:", ipcErr);
            });
        }
        client.connectSocket.send(prepareMessage({type: "connected", data: {}}), function() {
            callback(null, "connected");
        });
    });
}

export function shutdown(
    client: IPipeProcClient,
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.connectSocket) {
        //@ts-ignore
        client.connectSocket.close();
    }
    if (client.pipeProcNode) {
        d("closing node...");
        const shutDownMessage = prepareMessage({type: "system_shutdown"});
        const systemClosedListener = function(e: IPipeProcMessage) {
            if (e.msgKey !== shutDownMessage.msgKey) return;
            (<ChildProcess>client.pipeProcNode).removeListener("message", systemClosedListener);
            delete client.pipeProcNode;
            delete client.connectSocket;
            if (e.type === "system_closed") {
                d("node closed");
                callback(null, "closed");
            } else if (e.type === "system_closed_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        (<ChildProcess>client.pipeProcNode).on("message", systemClosedListener);
        (<ChildProcess>client.pipeProcNode).send(shutDownMessage);
    } else {
        return callback(new Error("no_active_node"));
    }
}

export function sendMessageToNode(
    client: IPipeProcClient,
    msg: IPipeProcMessage,
    callback?: (err?: Error) => void
): void {
    if (!client.pipeProcNode) {
        if (typeof callback === "function") {
            callback(new Error("no_active_node"));
        }
        return;
    }
    if (!client.connectSocket) {
        if (typeof callback === "function") {
            callback(new Error("no_active_ipc_channel"));
        }
        return;
    }
    client.connectSocket.send(msg, function() {
        if (typeof callback === "function") callback();
    });
}
