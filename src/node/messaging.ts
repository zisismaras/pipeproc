import debug from "debug";
import {
    prepareMessage,
    IPipeProcMessage
} from "../common/messages";
import {IWriteBuffer} from "./writeBuffer";
import {IWorker} from "./workerManager";
import {readFileSync} from "fs";

import {bind, ServerSocket} from "../socket/bind";

const d = debug("pipeproc:node");

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
        try {
            worker.process.send(msg, cb);
        } catch (e) {
            d(e);
        }
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
    address: string,
    tls: {
        key: string;
        cert: string;
        ca: string;
    } | false,
    callback: (err: Error | null, socketServer?: ServerSocket) => void
) {
    try {
        if (tls) {
            tls.ca = readFileSync(tls.ca, "utf8");
            tls.key = readFileSync(tls.key, "utf8");
            tls.cert = readFileSync(tls.cert, "utf8");
        }
    } catch (e) {
        return callback(e);
    }
    bind(address, {tls: tls}, function(err, server) {
        if (err) {
            return callback(err);
        }
        if (!server) {
            return callback(new Error("failed_to_create_server"));
        }
        server.onMessage<IPipeProcMessage>(function(msg, binder) {
            if (!registry[msg.type]) return;
            if (registry[msg.type].writeOp) {
                writeBuffer.push(function(cb) {
                    registry[msg.type].listener(msg.data, function(errStatus, reply) {
                        if (errStatus) {
                            binder.send(prepareMessage({
                                type: registry[msg.type].replyError,
                                msgKey: msg.msgKey,
                                errStatus: errStatus
                            }), cb);
                        } else {
                            binder.send(prepareMessage({
                                type: registry[msg.type].replySuccess,
                                msgKey: msg.msgKey,
                                data: reply
                            }), cb);
                        }
                    });
                });
            } else {
                registry[msg.type].listener(msg.data, function(errStatus, reply) {
                    if (errStatus) {
                        binder.send(prepareMessage({
                            type: registry[msg.type].replyError,
                            msgKey: msg.msgKey,
                            errStatus: errStatus
                        }));
                    } else {
                        binder.send(prepareMessage({
                            type: registry[msg.type].replySuccess,
                            msgKey: msg.msgKey,
                            data: reply
                        }));
                    }
                });
            }
        });

        callback(null, server);
    });
}
