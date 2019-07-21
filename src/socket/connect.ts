import {createConnection} from "net";
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
    options: {},
    callback: (err: Error | null, socketServer: ConnectSocket) => void
) {
    //tslint:disable no-any
    const messageListeners: MessageListener<any>[] = [];
    //tslint:enable no-any
    const errorListeners: MessageListener<Error>[] = [];
    const socketPath = address.replace("ipc://", "");
    const socket = createConnection(xpipe(socketPath));
    const connectSocket: ConnectSocket = {
        onError: function(listener) {
            errorListeners.push(listener);
        },
        onMessage: function(listener) {
            messageListeners.push(listener);
        },
        close: function() {
            socket.end();
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
            return callback(err, connectSocket);
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
