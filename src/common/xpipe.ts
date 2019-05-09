export function xpipe(path: string) {
    const prefix = getPrefix();
    if (prefix.endsWith("/") && path.startsWith("/")) {
        return prefix + path.substr(1);
    }
    return prefix + path;
}

function getPrefix() {
    return process.platform === "win32" ? "//./pipe/" : "";
}
