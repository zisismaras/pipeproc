import {forever} from "async";
import debug from "debug";
const d = debug("pipeproc:node");
export interface IWriteBuffer extends Array<(cb: () => void) => void> {}
export type WriteBufferStopper = (cb: () => void) => void;

export function startWriteBuffer(buffer: IWriteBuffer): WriteBufferStopper {
    d("Starting write buffer");
    let stop = false;
    let stopped = false;
    forever(function(next) {
        if (stop) return next("stop");
        if (buffer.length > 0) {
            d("buffer size:", buffer.length);
            const listenerWrapper = buffer.shift();
            if (listenerWrapper) {
                listenerWrapper(function() {
                    setTimeout(next, 10);
                });
            } else {
                setTimeout(next, 10);
            }
        } else {
            setTimeout(next, 10);
        }
    }, function() {
        stopped = true;
    });

    let timer: NodeJS.Timer;
    return function stopWriteBuffer(cb) {
        stop = true;
        if (!stopped) {
            timer = setTimeout(function() {
                d("stopping writeBuffer...");
                stopWriteBuffer(cb);
            }, 10);
        } else {
            d("writeBuffer stopped");
            if (timer) clearTimeout(timer);
            cb();
        }
    };
}
