import {Socket} from "net";

export function getSender(
    socket: Socket
) {
    return function(
        message: string | object,
        callback?: () => void
    ) {
        let msg: string;
        if (typeof message === "string") {
            msg = message;
        } else {
            msg = JSON.stringify(message);
        }
        msg += "%EOM%";
        const flushed = socket.write(msg, "utf8", function() {
            if (!flushed) {
                if (callback && typeof callback === "function") {
                    callback();
                }
            }
        });
        if (flushed) {
            if (callback && typeof callback === "function") {
                setImmediate(callback);
            }
        }
    };
}
