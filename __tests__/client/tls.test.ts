//tslint:disable
import "jest-extended";
import {satisfies} from "semver";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import net from "net";
import http from "http";
import {join as pathJoin} from "path";

const tlsSettings = {
    server: {
        ca: pathJoin(__dirname, "/keys/ca.pem"),
        key: pathJoin(__dirname, "/keys/server-key.pem"),
        cert: pathJoin(__dirname, "/keys/server-cert.pem")
    },
    client: {
        ca: pathJoin(__dirname, "/keys/ca.pem"),
        key: pathJoin(__dirname, "/keys/client-key.pem"),
        cert: pathJoin(__dirname, "/keys/client-cert.pem")
    }
};

describe("spawning a node using TLS", function() {
    let client: IPipeProcClient;
    let address: string;

    beforeEach(async function() {
        client = PipeProc();
        address = `tcp://127.0.0.1:${await getRandomPort()}`;
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should spawn correctly", async function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: address,
            tls: tlsSettings
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should be able to connect to an existing node", function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: address,
            tls: tlsSettings
        }).then(function() {
            const client2 = PipeProc();
            client2.connect({
                socket: address,
                tls: tlsSettings.client
            }).then(function(status) {
                expect(status).toEqual("connected");
                done();
            });
        });
    });

    it("shouldn't be able to connect to an existing node if not using tls", function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: address,
            tls: tlsSettings
        }).then(function() {
            const client2 = PipeProc();
            client2.connect({
                socket: address,
                timeout: 50
            }).then(function() {
                done.fail("it should not be able to connect");
            }).catch(function(err) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toBe("connection timed-out");
                done();
            });
        });
    });

    it("shouldn't be able to use tls with ipc", function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            tls: tlsSettings
        }).then(function() {
            done.fail("shouldn't be able to use tls with ipc");
        }).catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toMatch("Invalid connection address");
            done();
        });
    });

    it("shouldn't be able to connect with an invalid certificate", function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: address,
            tls: tlsSettings
        }).then(function() {
            const client2 = PipeProc();
            client2.connect({
                socket: address,
                timeout: 50,
                tls: {
                    ca: pathJoin(__dirname, "/keys/ca.pem"),
                    key: pathJoin(__dirname, "/keys/client-key.pem"),
                    cert: pathJoin(__dirname, "/keys/invalid-client-cert.pem")
                }
            }).then(function() {
                done.fail("it should not be able to connect");
            }).catch(function(err) {
                if (satisfies(process.version.replace("v", ""), ">=8.0.0 <10.0.0")) {
                    expect(err.message).toBe("socket hang up");
                } else if (satisfies(process.version.replace("v", ""), ">=10.0.0 <12.0.0")) {
                    expect(err.message).toBe("Client network socket disconnected before secure TLS connection was established");
                } else {
                    expect(err.message).toBe("connection timed-out");
                }
                done();
            });
        });
    });
});

function getRandomPort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const server = http.createServer();
        server.listen(0);
        server.once("listening", () => {
            const addressInfo = <net.AddressInfo>server.address();
            server.close(() => resolve(addressInfo.port));
        });
        server.once("error", reject);
    });
}
