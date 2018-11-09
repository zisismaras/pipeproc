import {forever} from "async";
import debug from "debug";
const d = debug("pipeproc:node");
export interface IWriteBuffer extends Array<(cb: () => void) => void> {}

export function startWriteBuffer(buffer: IWriteBuffer): void {
    d("Starting write buffer");
    var processing = false;
    forever(function(next) {
        if (buffer.length > 0 && !processing) {
            d("buffer size:", buffer.length);
            const listenerWrapper = buffer.shift();
            if (listenerWrapper) {
                listenerWrapper(function() {
                    processing = false;
                    setTimeout(next, 10);
                });
            } else {
                setTimeout(next, 10);
            }
        } else {
            setTimeout(next, 10);
        }
    }, function() {
        processing = false;
        startWriteBuffer(buffer);
    });
}
