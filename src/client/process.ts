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
        namespace: string,
        tcp: {
            host: string,
            port: number
        } | false,
        gc?: {minPruneTime?: number, interval?: number} | boolean
    },
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) return callback(null, "node_already_active");
    d("spawning node...");
    client.pipeProcNode = forkProcess(`${__dirname}/../node/pipeProc`);
    let initData;
    if (options.tcp) {
        initData = {tcp: options.tcp};
    } else {
        initData = {namespace: options.namespace};
    }
    const initIPCMessage = prepareMessage({type: "init_ipc", data: initData});
    const ipcEstablishedListener = function(e: IPipeProcIPCEstablishedMessage) {
        if (e.type !== "ipc_established" || e.msgKey !== initIPCMessage.msgKey) return;
        (<ChildProcess>client.pipeProcNode).removeListener("message", ipcEstablishedListener);
        const sock = zmq.socket("dealer");
        sock.identity = Buffer.from(String(process.pid));
        if (options.tcp) {
            sock.connect(`tcp://${options.tcp.host}:${options.tcp.port}`);
            d("tcp connection established on host:", options.tcp.host, "and port:", options.tcp.port);
        } else {
            sock.connect(`ipc://${tmpdir()}/pipeproc.${options.namespace}`);
            d("ipc established under namespace:", options.namespace);
        }
        //messageMap listener init
        sock.on("message", function(_del, rawMessage: Buffer) {
            const message: IPipeProcMessage = JSON.parse(rawMessage.toString());
            if (typeof client.messageMap[message.msgKey] === "function") {
                client.messageMap[message.msgKey](message);
                delete client.messageMap[message.msgKey];
            }
        });
        client.ipc = sock;
        sock.send(["", JSON.stringify(prepareMessage({type: "system_init", data: {options: options}}))]);
        callback(null, "spawned_and_connected");
    };
    (<ChildProcess>client.pipeProcNode).on("message", ipcEstablishedListener);
    (<ChildProcess>client.pipeProcNode).send(initIPCMessage);
}

export function connect(
    client: IPipeProcClient,
    options: {
        isWorker: boolean,
        namespace: string,
        tcp: {
            host: string,
            port: number
        } | false
    },
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.ipc) return callback(null, "already_connected");
    if (!client.pipeProcNode && options.isWorker) {
        client.pipeProcNode = process;
    } else if (!client.pipeProcNode) {
        client.pipeProcNode = {};
    }
    const sock = zmq.socket("dealer");
    sock.identity = Buffer.from(String(process.pid));
    if (options.tcp) {
        sock.connect(`tcp://${options.tcp.host}:${options.tcp.port}`);
        d("tcp connection established on host:", options.tcp.host, "and port:", options.tcp.port);
    } else {
        sock.connect(`ipc://${tmpdir()}/pipeproc.${options.namespace}`);
        d("ipc established under namespace:", options.namespace);
    }
    //messageMap listener init
    sock.on("message", function(_del, rawMessage: Buffer) {
        const message: IPipeProcMessage = JSON.parse(rawMessage.toString());
        if (typeof client.messageMap[message.msgKey] === "function") {
            client.messageMap[message.msgKey](message);
            delete client.messageMap[message.msgKey];
        }
    });
    client.ipc = sock;
    sock.send(["", JSON.stringify(prepareMessage({type: "connected", data: {}}))]);
    callback(null, "connected");
}

export function shutdown(
    client: IPipeProcClient,
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.ipc) {
        client.ipc.close();
    }
    //@ts-ignore
    if (client.pipeProcNode && typeof client.pipeProcNode.on === "function") {
        d("closing node...");
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
    } else if (client.pipeProcNode) {
        delete client.pipeProcNode;
        delete client.ipc;
        callback();
    } else {
        callback(new Error("no_active_node"));
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
    client.ipc.send(["", JSON.stringify(msg)]);
    if (typeof callback === "function") callback();
}
