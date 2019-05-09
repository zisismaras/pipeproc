import {fork as forkProcess, ChildProcess} from "child_process";
import debug from "debug";
import {prepareMessage, IPipeProcMessage, IPipeProcIPCEstablishedMessage} from "../common/messages";
import {IPipeProcClient} from ".";
import {tmpdir} from "os";
//@ts-ignore
import {Client} from "@crussell52/socket-ipc";
import {xpipe} from "../common/xpipe";

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
        if (e.type !== "ipc_established" || e.msgKey !== initIPCMessage.msgKey) return;
        d("ipc established under namespace:", client.namespace);
        (<ChildProcess>client.pipeProcNode).removeListener("message", ipcEstablishedListener);
        const ipcClient = new Client({socketFile: xpipe(`${tmpdir()}/pipeproc.${client.namespace || "default"}`)});
        ipcClient.on("connect", function() {
            d("connected to ipc channel");
            client.ipc = ipcClient;
            ipcClient.send("message",
                prepareMessage({type: "system_init", data: {options: options}}));
            callback(null, "spawned_and_connected");
        });
        //messageMap listener init
        ipcClient.on("message", function(message: IPipeProcMessage) {
            if (typeof client.messageMap[message.msgKey] === "function") {
                client.messageMap[message.msgKey](message);
                delete client.messageMap[message.msgKey];
            }
        });
        ipcClient.connect();
    };
    (<ChildProcess>client.pipeProcNode).on("message", ipcEstablishedListener);
    (<ChildProcess>client.pipeProcNode).send(initIPCMessage);
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
    const ipcClient = new Client({socketFile: xpipe(`${tmpdir()}/pipeproc.${client.namespace || "default"}`)});
    ipcClient.on("connect", function() {
        d("connected to ipc channel");
        client.ipc = ipcClient;
        ipcClient.send("message", prepareMessage({type: "connected", data: {}}));
        callback(null, "connected");
    });
    //messageMap listener init
    ipcClient.on("message", function(message: IPipeProcMessage) {
        if (typeof client.messageMap[message.msgKey] === "function") {
            client.messageMap[message.msgKey](message);
            delete client.messageMap[message.msgKey];
        }
    });
    ipcClient.connect();
}

export function shutdown(
    client: IPipeProcClient,
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) {
        d("closing node...");
        const msg = prepareMessage({type: "system_shutdown"});
        client.messageMap[msg.msgKey] = function(e: IPipeProcMessage) {
            if (e.type === "system_closed") {
                d("node closed");
                delete client.pipeProcNode;
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
    //@ts-ignore
    client.ipc.send("message", msg);
    if (typeof callback === "function") callback();
}
