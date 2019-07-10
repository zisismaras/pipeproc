import {
    prepareMessage,
    IPipeProcMessage
} from "../common/messages";
import {IWriteBuffer} from "./writeBuffer";
import {IWorker} from "./workerManager";
import {tmpdir} from "os";
import zmq from "zeromq";

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
    const sock = zmq.socket("router");
    sock.bindSync(`ipc://${tmpdir()}/pipeproc.${namespace || "default"}`);
    sock.on("message", function(identity, _del, rawMessage: Buffer) {
        const msg: IPipeProcMessage = JSON.parse(rawMessage.toString());
        if (!registry[msg.type]) return;
        if (registry[msg.type].writeOp) {
            writeBuffer.push(function(cb) {
                registry[msg.type].listener(msg.data, function(errStatus, reply) {
                    if (errStatus) {
                        sock.send([identity, "", JSON.stringify(prepareMessage({
                            type: registry[msg.type].replyError,
                            msgKey: msg.msgKey,
                            errStatus: errStatus
                        }))]);
                        setImmediate(cb);
                    } else {
                        sock.send([identity, "", JSON.stringify(prepareMessage({
                            type: registry[msg.type].replySuccess,
                            msgKey: msg.msgKey,
                            data: reply
                        }))]);
                        setImmediate(cb);
                    }
                });
            });
        } else {
            registry[msg.type].listener(msg.data, function(errStatus, reply) {
                if (errStatus) {
                    sock.send(JSON.stringify(prepareMessage({
                        type: registry[msg.type].replyError,
                        msgKey: msg.msgKey,
                        errStatus: errStatus
                    })));
                } else {
                    sock.send(JSON.stringify(prepareMessage({
                        type: registry[msg.type].replySuccess,
                        msgKey: msg.msgKey,
                        data: reply
                    })));
                }
            });
        }
    });

    return sock;
}
