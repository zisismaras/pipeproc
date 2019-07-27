import {createConnection, Socket} from "net";
import {connect as createTlsConnection, TLSSocket} from "tls";
import {getSender} from "./send";
import {xpipe} from "./xpipe";
import debug from "debug";

type MessageListener<T> = (msg: T) => void;

export type ConnectSocket = {
    onError: (listener: MessageListener<Error>) => void;
    onMessage: <T>(listener: MessageListener<T>) => void;
    close: () => void;
    send: (message: string | object, callback?: () => void) => void;
};

const d = debug("pipeproc:socket:connect");

export function connect(
    address: string,
    options: {
        tls: {
            key: string;
            cert: string;
            ca: string;
        } | false
    },
    callback: (err: Error | null, socketServer?: ConnectSocket) => void
) {
    //tslint:disable no-any
    const messageListeners: MessageListener<any>[] = [];
    //tslint:enable no-any
    const errorListeners: MessageListener<Error>[] = [];
    let socket: Socket | TLSSocket;
    if (options.tls) {
        if (address.includes("tcp://")) {
            const parts = address.replace("tcp://", "").split(":");
            socket = createTlsConnection({
                port: parseInt(parts[1]),
                host: parts[0],
                ca: options.tls.ca,
                key: options.tls.key,
                cert: options.tls.cert
            });
        } else {
            return callback(new Error(`Invalid connection address: ${address}`));
        }
    } else {
        if (address.includes("ipc://")) {
            const socketPath = address.replace("ipc://", "");
            socket = createConnection(xpipe(socketPath));
        } else if (address.includes("tcp://")) {
            const parts = address.replace("tcp://", "").split(":");
            socket = createConnection({
                port: parseInt(parts[1]),
                host: parts[0]
            });
        } else {
            return callback(new Error(`Invalid connection address: ${address}`));
        }
    }
    const connectSocket: ConnectSocket = {
        onError: function(listener) {
            errorListeners.push(listener);
        },
        onMessage: function(listener) {
            messageListeners.push(listener);
        },
        close: function() {
            socket.destroy();
        },
        send: getSender(socket)
    };
    let cbCalled = false;
    socket.once("connect", function() {
        socket.setNoDelay(true);
        socket.setEncoding("utf8");
        if (!cbCalled) {
            cbCalled = true;
            callback(null, connectSocket);
        }
    });
    socket.on("error", function(err) {
        d("Socket error:", err);
        if (!cbCalled) {
            cbCalled = true;
            return callback(err);
        }
        errorListeners.forEach(function(listener) {
            listener(err);
        });
    });
    let buffer = "";
    socket.on("data", function(chunk: string) {
        const data = buffer + chunk;
        const messages = data.split("%EOM%");
        buffer = messages.pop() || "";
        messages.forEach(function(msg) {
            let parsedData: object;
            try {
                parsedData = JSON.parse(msg);
            } catch (e) {
                d("Socket data parsing error:", e, msg);
                return;
            }
            messageListeners.forEach(function(listener) {
                listener(parsedData);
            });
        });
    });
}
