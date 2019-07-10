import {fork as forkProcess, ChildProcess} from "child_process";
import debug from "debug";
import {prepareMessage, IPipeProcMessage, IPipeProcIPCEstablishedMessage} from "../common/messages";
import {IPipeProcClient} from ".";
import {tmpdir} from "os";
import zmq from "zeromq";

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
        const sock = zmq.socket("req");
        sock.connect(`ipc://${tmpdir()}/pipeproc.${client.namespace || "default"}`);
        //messageMap listener init
        sock.on("message", function(rawMessage: Buffer) {
            const message: IPipeProcMessage = JSON.parse(rawMessage.toString());
            if (typeof client.messageMap[message.msgKey] === "function") {
                client.messageMap[message.msgKey](message);
                delete client.messageMap[message.msgKey];
            }
        });
        client.ipc = sock;
        sock.send(JSON.stringify(prepareMessage({type: "system_init", data: {options: options}})));
        callback(null, "spawned_and_connected");
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
    const sock = zmq.socket("req");
    sock.connect(`ipc://${tmpdir()}/pipeproc.${client.namespace || "default"}`);
    //messageMap listener init
    sock.on("message", function(rawMessage: Buffer) {
        const message: IPipeProcMessage = JSON.parse(rawMessage.toString());
        console.log("CLIENT", message);
        if (typeof client.messageMap[message.msgKey] === "function") {
            client.messageMap[message.msgKey](message);
            delete client.messageMap[message.msgKey];
        }
    });
    client.ipc = sock;
    sock.send(JSON.stringify(prepareMessage({type: "connected", data: {}})));
    callback(null, "connected");
}

export function shutdown(
    client: IPipeProcClient,
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) {
        d("closing node...");
        //@ts-ignore
        client.ipc.disconnect(`ipc://${tmpdir()}/pipeproc.${client.namespace || "default"}`);
        const shutDownMessage = prepareMessage({type: "system_shutdown"});
        const systemClosedListener = function(e: IPipeProcMessage) {
            if (e.msgKey !== shutDownMessage.msgKey) return;
            (<ChildProcess>client.pipeProcNode).removeListener("message", systemClosedListener);
            delete client.pipeProcNode;
            delete client.ipc;
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
    if (!client.ipc) {
        if (typeof callback === "function") {
            callback(new Error("no_active_ipc_channel"));
        }
        return;
    }
    //@ts-ignore
    client.ipc.send(JSON.stringify(msg));
    if (typeof callback === "function") callback();
}
