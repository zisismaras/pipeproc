//tslint:disable
import "jest-extended";
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
            }).then(function(status) {
                done.fail("it should not be able to connect");
            }).catch(function(err) {
                expect(err.message).toBe("connection timed-out");
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
