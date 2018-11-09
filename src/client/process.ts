import {spawn as spawnProcess} from "child_process";
import debug from "debug";
import {prepareMessage, IPipeProcMessage} from "../common/messages";
import {IPipeProcClient} from ".";
import {IPC} from "node-ipc";

const d = debug("pipeproc:client");

export function spawn(
    client: IPipeProcClient,
    options: {memory: boolean, location: string, workers: number},
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) return callback(new Error("node_already_active"));
    d("spawning node...");
    client.pipeProcNode = spawnProcess("node", [`${__dirname}/../node/pipeProc`], {stdio: "inherit"});
    const ipc = new IPC();
    ipc.config.id = "pipeproc";
    ipc.config.retry = 100;
    ipc.config.silent = false;
    ipc.config.logger = debug("pipeproc:ipc:client");
    ipc.connectTo("pipeproc", function() {
        ipc.of.pipeproc.on("connect", function() {
            d("connected to ipc channel");
            client.ipc = ipc;
            ipc.of.pipeproc.emit("message", prepareMessage({type: "system_init", data: {options: options}}));
            callback(null, "spawned_and_connected");
        });
        ipc.of.pipeproc.on("disconnect", function() {
            d("disconnected from ipc channel");
        });
        //messageMap listener init
        ipc.of.pipeproc.on("message", function(message: IPipeProcMessage) {
            if (typeof client.messageMap[message.msgKey] === "function") {
                client.messageMap[message.msgKey](message);
                delete client.messageMap[message.msgKey];
            }
        });
    });
}

export function connect(
    client: IPipeProcClient,
    options: {isWorker: boolean},
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.ipc) return callback(null, "already_connected");
    if (!client.pipeProcNode && options.isWorker) {
        client.pipeProcNode = process;
    } else if (!client.pipeProcNode) {
        client.pipeProcNode = {};
    }
    const ipc = new IPC();
    ipc.config.id = "pipeproc";
    ipc.config.retry = 100;
    ipc.config.silent = false;
    if (options.isWorker) {
        ipc.config.logger = debug("pipeproc:ipc:worker");
    } else {
        ipc.config.logger = debug("pipeproc:ipc:client");
    }
    ipc.connectTo("pipeproc", function() {
        ipc.of.pipeproc.on("connect", function() {
            d("connected to ipc channel");
            client.ipc = ipc;
            ipc.of.pipeproc.emit("message", prepareMessage({type: "connected", data: {}}));
            callback(null, "connected");
        });
        ipc.of.pipeproc.on("disconnect", function() {
            d("disconnected from ipc channel");
        });
        //messageMap listener init
        ipc.of.pipeproc.on("message", function(message: IPipeProcMessage) {
            if (typeof client.messageMap[message.msgKey] === "function") {
                client.messageMap[message.msgKey](message);
                delete client.messageMap[message.msgKey];
            }
        });
    });
}

export function shutdown(
    client: IPipeProcClient,
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) {
        d("closing node...");
        if (client.ipc) client.ipc.config.stopRetrying = true;
        const msg = prepareMessage({type: "system_shutdown"});
        client.messageMap[msg.msgKey] = function(e: IPipeProcMessage) {
            if (e.type === "system_closed") {
                d("node closed");
                callback(null, "closed");
            } else if (e.type === "system_closed_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        sendMessageToNode(client, msg);
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
    if (!client.ipc) {
        if (typeof callback === "function") {
            callback(new Error("no_active_ipc_channel"));
        }
        return;
    }
    client.ipc.of.pipeproc.emit("message", msg);
    if (typeof callback === "function") callback();
}
