export const ZERO_TONE = "0000000000000000";
export const FIRST_TONE = "0000000000000001";
export const SECOND_TONE = "0000000000000002";

export function incrementCurrentTone(currentTone: string): string {
    return zeroPad(parseInt(currentTone) + 1);
}

export function decrementCurrentTone(currentTone: string): string {
    const tone = parseInt(currentTone) - 1;
    if (tone <= 0) {
        return ZERO_TONE;
    } else {
        return zeroPad(tone);
    }
}

export function convertToClientId(nodeId: string): string {
    if (!nodeId) return nodeId;
    return nodeId.split("..").map(function(id) {
        const idParts = id.split("-");
        const parsedId = parseInt(idParts[1]);
        if (parsedId <= 0) {
            return `${idParts[0]}-0`;
        } else {
            return `${idParts[0]}-${parsedId - 1}`;
        }
    }).join("..");
}

export function convertRangeParams(param: string): string {
    if (!param) return param;
    if (param.includes(":")) {
        const parsed = parseInt(param.replace(":", ""));
        return `:${parsed + 1}`;
    } else if (param.includes("-")) {
        const parts = param.split("-");
        return `${parts[0]}-${incrementCurrentTone(parts[1])}`;
    } else {
        return param;
    }
}

export function zeroPad(input: string | number): string {
    const parsed = String(input);
    const zerosToFill = 16 - parsed.length;

    return (new Array(zerosToFill)).fill("0").join("") + parsed;
}
