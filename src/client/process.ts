import debug from "debug";
import {prepareMessage, IPipeProcMessage, IPipeProcIPCEstablishedMessage, IPipeProcPingMessageReply} from "../common/messages";
import {IPipeProcClient} from ".";
import {tmpdir} from "os";
import {readFileSync} from "fs";

import {connect as socketConnect} from "../socket/connect";
import {Monitor} from "forever-monitor";
import {join as pathJoin} from "path";

const d = debug("pipeproc:client");

export function spawn(
    client: IPipeProcClient,
    options: {
        address: string,
        namespace: string,
        tcp: {
            host: string,
            port: number
        } | false,
        memory: boolean,
        location: string,
        workers: number,
        workerConcurrency: number,
        workerRestartAfter: number,
        gc?: {minPruneTime?: number, interval?: number} | boolean,
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
    },
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.pipeProcNode) return callback(null, "node_already_active");
    d("spawning node...");
    client.pipeProcNode = new Monitor(pathJoin(__dirname, "..", "node", "pipeProc.js"), {
        //@ts-ignore
        fork: true,
        watch: false,
        //@ts-ignore
        spawnWith: {detached: true},
        args: ["--color"],
        max: 3
    });
    (<Monitor>client.pipeProcNode).start();
    let connectionAddress: string;
    if (options.address) {
        connectionAddress = options.address;
    } else if (options.tcp) {
        connectionAddress = `tcp://${options.tcp.host}:${options.tcp.port}`;
    } else {
        connectionAddress = `ipc://${tmpdir()}/pipeproc.${options.namespace}`;
    }
    function spawnNode() {
        const initIPCMessage = prepareMessage({type: "init_ipc", data: {address: connectionAddress, tls: options.tls}});
        const ipcEstablishedListener = function(e: IPipeProcIPCEstablishedMessage) {
            if ((e.type !== "ipc_established") || e.msgKey !== initIPCMessage.msgKey) return;
            d("internal IPC enabled");
            if (e.errStatus) {
                return callback(new Error(e.errStatus));
            }
            try {
                if (options.tls) {
                    options.tls.client.ca = readFileSync(options.tls.client.ca, "utf8");
                    options.tls.client.key = readFileSync(options.tls.client.key, "utf8");
                    options.tls.client.cert = readFileSync(options.tls.client.cert, "utf8");
                }
            } catch (e) {
                return callback(e);
            }
            (<Monitor>client.pipeProcNode).child.removeListener("message", ipcEstablishedListener);
            socketConnect(connectionAddress, {tls: options.tls && options.tls.client}, function(err, connectSocket) {
                if (err) {
                    return callback(err);
                }
                if (!connectSocket) {
                    return callback(new Error("Failed to create socket"));
                }
                client.connectSocket = connectSocket;
                if (options.address) {
                    d("using connection address:", options.address);
                } else if (options.tcp) {
                    d("tcp connection established on host:", options.tcp.host, "and port:", options.tcp.port);
                } else {
                    d("ipc established under namespace:", options.namespace);
                }
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
                const systemInitMessage = prepareMessage({type: "system_init", data: {options: options}});
                client.messageMap[systemInitMessage.msgKey] = function(systemInitReply: IPipeProcMessage) {
                    if (systemInitReply.type === "system_ready") {
                        callback(null, "spawned_and_connected");
                    } else if (systemInitReply.type === "system_ready_error") {
                        callback(new Error(e.errStatus));
                    }
                };
                d("sending system_init message");
                sendMessageToNode(client, systemInitMessage);
            });
        };
        (<Monitor>client.pipeProcNode).child.on("message", ipcEstablishedListener);
        (<Monitor>client.pipeProcNode).child.send(initIPCMessage);
    }
    (<Monitor>client.pipeProcNode).on("restart", spawnNode);
    spawnNode();
}

export function connect(
    client: IPipeProcClient,
    options: {
        address: string,
        namespace: string,
        tcp: {
            host: string,
            port: number
        } | false,
        isWorker: boolean,
        tls: {
            key: string;
            cert: string;
            ca: string;
        } | false;
        timeout: number
    },
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.connectSocket) return callback(null, "already_connected");
    if (!client.pipeProcNode && options.isWorker) {
        client.pipeProcNode = process;
    } else if (!client.pipeProcNode) {
        client.pipeProcNode = {};
    }
    let connectionAddress: string;
    if (options.address) {
        connectionAddress = options.address;
    } else if (options.tcp) {
        connectionAddress = `tcp://${options.tcp.host}:${options.tcp.port}`;
    } else {
        connectionAddress = `ipc://${tmpdir()}/pipeproc.${options.namespace}`;
    }
    try {
        if (options.tls) {
            options.tls.ca = readFileSync(options.tls.ca, "utf8");
            options.tls.key = readFileSync(options.tls.key, "utf8");
            options.tls.cert = readFileSync(options.tls.cert, "utf8");
        }
    } catch (e) {
        return callback(e);
    }
    socketConnect(connectionAddress, {tls: options.tls}, function(err, connectSocket) {
        if (err) {
            return callback(err);
        }
        if (!connectSocket) {
            return callback(new Error("Failed to create socket"));
        }
        client.connectSocket = connectSocket;
        if (options.address) {
            d("using connection address:", options.address);
        } else if (options.tcp) {
            d("tcp connection established on host:", options.tcp.host, "and port:", options.tcp.port);
        } else {
            d("ipc established under namespace:", options.namespace);
        }
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
        const pingMessage = prepareMessage({type: "ping", data: {}});
        let gotPong = false;
        setTimeout(function() {
            if (!gotPong) {
                callback(new Error("connection timed-out"));
            }
        }, options.timeout);
        client.messageMap[pingMessage.msgKey] = function(e: IPipeProcPingMessageReply) {
            if (e.type === "pong") {
                gotPong = true;
                callback(null, "connected");
            }
        };
        d("sending ping message");
        sendMessageToNode(client, pingMessage);
    });
}

export function shutdown(
    client: IPipeProcClient,
    callback: (err?: Error | null, status?: string) => void
): void {
    if (client.connectSocket) {
        client.connectSocket.close();
    }
    if (client.pipeProcNode instanceof Monitor) {
        d("closing node...");
        const shutDownMessage = prepareMessage({type: "system_shutdown"});
        const systemClosedListener = function(e: IPipeProcMessage) {
            if (e.msgKey !== shutDownMessage.msgKey) return;
            (<Monitor>client.pipeProcNode).child.removeListener("message", systemClosedListener);
            (<Monitor>client.pipeProcNode).stop();
            delete client.pipeProcNode;
            delete client.connectSocket;
            if (e.type === "system_closed") {
                d("node closed");
                callback(null, "closed");
            } else if (e.type === "system_closed_error") {
                callback(new Error(e.errStatus || "uknown_error"));
            }
        };
        client.pipeProcNode.child.on("message", systemClosedListener);
        client.pipeProcNode.child.send(shutDownMessage);
    } else if (client.pipeProcNode) {
        d("disconnected");
        delete client.pipeProcNode;
        delete client.connectSocket;
        callback(null, "disconnected");
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
