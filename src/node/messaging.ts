import {
    prepareMessage,
    IPipeProcMessage
} from "../common/messages";
import {IWriteBuffer} from "./writeBuffer";
import {IWorker} from "./workerManager";
import {tmpdir} from "os";
import debug from "debug";

//@ts-ignore
import {Server} from "@crussell52/socket-ipc";
import {xpipe} from "../common/xpipe";

export interface IMessageRegistry {
    [key: string]: {
        messageType: string;
        replySuccess: string;
        replyError: string;
        writeOp: boolean;
        //tslint:disable no-any
        listener: messageListener<any, any>;
        //tslint:enable
    };
}

export type message<T, U> = {
    messageType: string;
    replySuccess: string;
    replyError: string;
    writeOp: boolean;
    listener: messageListener<T, U>;
};

export type messageListener<T, U> = (
    (
        data: T,
        callback: (
            errStatus?: string | null,
            reply?: U
        ) => void
    )
=> void);

export function sendMessageToWorker(msg: IPipeProcMessage, worker: IWorker, cb?: () => void): void {
    if (worker.process && typeof worker.process.send === "function") {
        worker.process.send(msg, cb);
    }
}

export function registerMessage<T, U>(registry: IMessageRegistry, newMessage: message<T, U>): void {
    if (!registry[newMessage.messageType]) {
        registry[newMessage.messageType] = newMessage;
    }
}

export function initializeMessages(
    writeBuffer: IWriteBuffer,
    registry: IMessageRegistry,
    namespace: string
) {
    const server = new Server({socketFile: xpipe(`${tmpdir()}/pipeproc.${namespace || "default"}`)});
    const d = debug("pipeproc:ipc:node");
    server.on("error", function(err: Error) {
        d("node IPC error:", err);
    });
    server.on("message", function(msg: IPipeProcMessage, _topic: string, clientId: string) {
        if (!registry[msg.type]) return;
        if (registry[msg.type].writeOp) {
            writeBuffer.push(function(cb) {
                registry[msg.type].listener(msg.data, function(errStatus, reply) {
                    if (errStatus) {
                        server.send("message", prepareMessage({
                            type: registry[msg.type].replyError,
                            msgKey: msg.msgKey,
                            errStatus: errStatus
                        }), clientId);
                        setImmediate(cb);
                    } else {
                        server.send("message", prepareMessage({
                            type: registry[msg.type].replySuccess,
                            msgKey: msg.msgKey,
                            data: reply
                        }), clientId);
                        setImmediate(cb);
                    }
                });
            });
        } else {
            registry[msg.type].listener(msg.data, function(errStatus, reply) {
                if (errStatus) {
                    server.send("message", prepareMessage({
                        type: registry[msg.type].replyError,
                        msgKey: msg.msgKey,
                        errStatus: errStatus
                    }), clientId);
                } else {
                    server.send("message", prepareMessage({
                        type: registry[msg.type].replySuccess,
                        msgKey: msg.msgKey,
                        data: reply
                    }), clientId);
                }
            });
        }
    });
    server.listen();

    return server;
}
