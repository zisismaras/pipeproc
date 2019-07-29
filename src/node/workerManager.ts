import {fork, ChildProcess} from "child_process";
import {timesSeries, series} from "async";
import {
    prepareWorkerInitMessage,
    prepareRegisterSystemProcsMessage,
    IPipeProcWorkerInitMessageReply,
    IPipeProcRegisterSystemProcsMessageReply
} from "../common/messages";
import {sendMessageToWorker} from "./messaging";
import {IProc} from "./proc";
import {ISystemProc} from "./systemProc";

import debug from "debug";
const d = debug("pipeproc:node");

export interface IWorker {
    process: ChildProcess;
}
export function spawnWorkers(
    workers: number,
    activeWorkers: IWorker[],
    activeProcs: IProc[],
    activeSystemProcs: ISystemProc[],
    address: string,
    clientTLS: {
        key: string;
        cert: string;
        ca: string;
    } | false,
    workerConcurrency: number,
    callback: (err?: Error | null) => void
): void {
    if (!workers) return process.nextTick(callback);
    d("spawing workers...");
    timesSeries(workers, function(i, next) {
        d("workers:", i + 1, "/", workers);
        const worker = {process: fork(`${__dirname}/worker/worker`)};
        series([
            function(cb) {
                const msg = prepareWorkerInitMessage(address, clientTLS, workerConcurrency);
                const listener = function(e: IPipeProcWorkerInitMessageReply) {
                    if (e.msgKey === msg.msgKey) {
                        worker.process.removeListener("message", listener);
                        if (e.type === "worker_connected") {
                            d("worker", worker.process.pid, "connected!");
                            activeWorkers.push(worker);
                            cb();
                        } else {
                            cb(new Error(e.errStatus || "uknown_worker_spawn_error"));
                        }
                    }
                };
                worker.process.on("message", listener);
                sendMessageToWorker(msg, worker);
            },
            function(cb) {
                const msg = prepareRegisterSystemProcsMessage(
                    activeProcs.filter(p => activeSystemProcs.find(sp => sp.name === p.name)),
                    activeSystemProcs
                );
                const listener = function(e: IPipeProcRegisterSystemProcsMessageReply) {
                    if (e.msgKey === msg.msgKey) {
                        worker.process.removeListener("message", listener);
                        if (e.type === "register_system_proc_ok") {
                            cb();
                        } else {
                            cb(new Error(e.errStatus || "register_system_proc_uknown_error"));
                        }
                    }
                };
                worker.process.on("message", listener);
                sendMessageToWorker(msg, worker);
            }
        ], next);
    }, function(err) {
        if (err) {
            callback(<Error>err);
        } else {
            callback();
        }
    });
}
