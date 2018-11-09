import {
    prepareMessage,
    IPipeProcMessage
} from "../common/messages";
import {IWriteBuffer} from "./writeBuffer";
import {IWorker} from "./workerManager";
import {IPC} from "node-ipc";
import debug from "debug";

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
    registry: IMessageRegistry
) {
    const ipc = new IPC();
    ipc.config.id = "pipeproc";
    ipc.config.retry = 100;
    ipc.config.silent = false;
    ipc.config.logger = debug("pipeproc:ipc:node");
    ipc.serve(function() {
        ipc.server.on("message", function(msg: IPipeProcMessage, socket) {
            if (!registry[msg.type]) return;
            if (registry[msg.type].writeOp) {
                writeBuffer.push(function(cb) {
                    registry[msg.type].listener(msg.data, function(errStatus, reply) {
                        if (errStatus) {
                            ipc.server.emit(socket, "message", prepareMessage({
                                type: registry[msg.type].replyError,
                                msgKey: msg.msgKey,
                                errStatus: errStatus
                            }));
                            setImmediate(cb);
                        } else {
                            ipc.server.emit(socket, "message", prepareMessage({
                                type: registry[msg.type].replySuccess,
                                msgKey: msg.msgKey,
                                data: reply
                            }));
                            setImmediate(cb);
                        }
                    });
                });
            } else {
                registry[msg.type].listener(msg.data, function(errStatus, reply) {
                    if (errStatus) {
                        ipc.server.emit(socket, "message", prepareMessage({
                            type: registry[msg.type].replyError,
                            msgKey: msg.msgKey,
                            errStatus: errStatus
                        }));
                    } else {
                        ipc.server.emit(socket, "message", prepareMessage({
                            type: registry[msg.type].replySuccess,
                            msgKey: msg.msgKey,
                            data: reply
                        }));
                    }
                });
            }
        });
    });
    ipc.server.start();
}
