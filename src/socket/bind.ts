import {createServer, Socket, Server} from "net";
import {createServer as createTlsServer, TLSSocket} from "tls";
import debug from "debug";
import {v4 as uuid} from "uuid";
import {unlinkSync} from "fs";
import {xpipe} from "./xpipe";
import {getSender} from "./send";

const d = debug("pipeproc:socket:bind");

type Binder = {
    id: string;
    send: (message: string | object, callback?: () => void) => void;
    buffer: string;
};

type MessageListener<T> = (data: T, binder: Binder) => void;

export type ServerSocket = {
    close: () => void;
    onMessage: <T>(listener: MessageListener<T>) => void;
};

export function bind(
    address: string,
    options: {
        tls: {
            key: string;
            cert: string;
            ca: string;
        } | false
    },
    callback: (err: Error | null, socketServer?: ServerSocket) => void
) {
    function connectionHandler(socket: Socket | TLSSocket) {
        d("new connection", socket.address());
        const binder = createBinder(socket);
        socketMap.set(socket, binder);
        socket.setEncoding("utf8");
        socket.setNoDelay(true);
        socket.on("end", function() {
            socketMap.delete(socket);
        });
        socket.on("close", function() {
            socketMap.delete(socket);
        });
        socket.on("data", function(chunk: string) {
            const data = binder.buffer + chunk;
            const messages = data.split("%EOM%");
            binder.buffer = messages.pop() || "";
            messages.forEach(function(msg) {
                let parsedData: object;
                try {
                    parsedData = JSON.parse(msg);
                } catch (e) {
                    d("Socket data parsing error:", e, msg);
                    return;
                }
                messageListeners.forEach(function(listener) {
                    listener(parsedData, binder);
                });
            });
        });
    }
    //tslint:disable no-any
    const messageListeners: MessageListener<any>[] = [];
    //tslint:enable no-any
    const socketMap: Map<Socket | TLSSocket, Binder> = new Map();
    const server = options.tls ? createTlsServer({
        ca: options.tls.ca,
        key: options.tls.key,
        cert: options.tls.cert,
        requestCert: true,
        rejectUnauthorized: true
    }, connectionHandler) : createServer(connectionHandler);
    const socketServer: ServerSocket = {
        close: function() {
            if (address.includes("ipc://")) {
                const socketPath = address.replace("ipc://", "");
                try {
                    unlinkSync(socketPath);
                } catch (_e) {}
            }
            server.close();
            d("message socket closed");
        },
        onMessage: function(listener) {
            messageListeners.push(listener);
        }
    };
    let connectionRetries = 0;
    let cbCalled = false;
    server.on("error", function(err: Error & {code: string}) {
        if (err.code === "EADDRINUSE" && address.includes("ipc://") && connectionRetries < 3) {
            const socketPath = address.replace("ipc://", "");
            try {
                unlinkSync(socketPath);
                setTimeout(function() {
                    connectionRetries += 1;
                    startServer(server, address);
                }, 100);
            } catch (_e) {}
        } else {
            d("Socket error:", err);
            if (!cbCalled) {
                cbCalled = true;
                callback(err);
            }
        }
    });
    server.once("listening", function() {
        d("Socket server is listening on", address);
        if (!cbCalled) {
            cbCalled = true;
            callback(null, socketServer);
        }
    });

    startServer(server, address);
}

function startServer(server: Server, address: string) {
    if (address.includes("ipc://")) {
        const socketPath = address.replace("ipc://", "");
        server.listen(xpipe(socketPath));
    } else if (address.includes("tcp://")) {
        const parts = address.replace("tcp://", "").split(":");
        server.listen(parseInt(parts[1]), parts[0]);
    }
}

function createBinder(socket: Socket | TLSSocket): Binder {
    return {
        id: uuid(),
        send: getSender(socket),
        buffer: ""
    };
}
