//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";
import {tmpdir} from "os";
import net from "net";
import http from "http";

describe("spawning the node with ipc namespace", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should spawn correctly", function(done) {
        client.spawn({memory: true, workers: 0, namespace: uuid()}).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should return a warning if spawned twice", function(done) {
        client.spawn({memory: true, workers: 0, namespace: uuid()}).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            client.spawn({memory: true}).then(function(status2) {
                expect(status2).toEqual("node_already_active");
                done();
            }).catch(function(err) {
                done.fail(err);
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });
});

describe("spawning the node with ipc address", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should spawn correctly", function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: `ipc://${tmpdir()}/pipeproc.${uuid()}`
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should return a warning if spawned twice", function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: `ipc://${tmpdir()}/pipeproc.${uuid()}`
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            client.spawn({memory: true}).then(function(status2) {
                expect(status2).toEqual("node_already_active");
                done();
            }).catch(function(err) {
                done.fail(err);
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });
});

describe("spawning the node with tcp address", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
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
            socket: `tcp://127.0.0.1:${await getRandomPort()}`
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should return a warning if spawned twice", async function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            socket: `tcp://127.0.0.1:${await getRandomPort()}`
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            client.spawn({memory: true}).then(function(status2) {
                expect(status2).toEqual("node_already_active");
                done();
            }).catch(function(err) {
                done.fail(err);
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });
});

describe("spawning the node with tcp options", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
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
            tcp: {
                host: "127.0.0.1",
                port: await getRandomPort()
            }
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should return a warning if spawned twice", async function(done) {
        client.spawn({
            memory: true,
            workers: 0,
            tcp: {
                host: "127.0.0.1",
                port: await getRandomPort()
            }
        }).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            client.spawn({memory: true}).then(function(status2) {
                expect(status2).toEqual("node_already_active");
                done();
            }).catch(function(err) {
                done.fail(err);
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });
});

describe("connecting to an existing node with ipc namespace", function() {
    let client: IPipeProcClient;
    let namespace: string;

    beforeEach(function(done) {
        client = PipeProc();
        namespace = uuid();
        client.spawn({memory: true, workers: 0, namespace: namespace}).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should be able to connect to an existing node", function(done) {
        const client2 = PipeProc();
        client2.connect({namespace: namespace}).then(function(status) {
            expect(status).toEqual("connected");
            done();
        });
    });

    it("should allow multiple clients to connect", async function() {
        const client2 = PipeProc();
        const client3 = PipeProc();
        const status = await client2.connect({namespace: namespace});
        expect(status).toEqual("connected");
        const status2 = await client3.connect({namespace: namespace});
        expect(status2).toEqual("connected");
    });

    it("should return a notice if we are already connected", function(done) {
        client.connect({namespace: namespace}).then(function(status) {
            expect(status).toEqual("already_connected");
            done();
        });
    });
});

describe("connecting to an existing node with ipc address", function() {
    let client: IPipeProcClient;
    let socket: string;

    beforeEach(function(done) {
        client = PipeProc();
        socket = `ipc://${tmpdir()}/pipeproc.${uuid()}`;
        client.spawn({
            memory: true,
            workers: 0,
            socket: socket
        }).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should be able to connect to an existing node", function(done) {
        const client2 = PipeProc();
        client2.connect({socket: socket}).then(function(status) {
            expect(status).toEqual("connected");
            done();
        });
    });

    it("should allow multiple clients to connect", async function() {
        const client2 = PipeProc();
        const client3 = PipeProc();
        const status = await client2.connect({socket: socket});
        expect(status).toEqual("connected");
        const status2 = await client3.connect({socket: socket});
        expect(status2).toEqual("connected");
    });

    it("should return a notice if we are already connected", function(done) {
        client.connect({socket: socket}).then(function(status) {
            expect(status).toEqual("already_connected");
            done();
        });
    });
});

describe("connecting to an existing node with tcp address", function() {
    let client: IPipeProcClient;
    let socket: string;

    beforeEach(async function(done) {
        client = PipeProc();
        socket = `tcp://127.0.0.1:${await getRandomPort()}`;
        client.spawn({
            memory: true,
            workers: 0,
            socket: socket
        }).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should be able to connect to an existing node", function(done) {
        const client2 = PipeProc();
        client2.connect({socket: socket}).then(function(status) {
            expect(status).toEqual("connected");
            done();
        });
    });

    it("should allow multiple clients to connect", async function() {
        const client2 = PipeProc();
        const client3 = PipeProc();
        const status = await client2.connect({socket: socket});
        expect(status).toEqual("connected");
        const status2 = await client3.connect({socket: socket});
        expect(status2).toEqual("connected");
    });

    it("should return a notice if we are already connected", function(done) {
        client.connect({socket: socket}).then(function(status) {
            expect(status).toEqual("already_connected");
            done();
        });
    });
});

describe("connecting to an existing node with tcp options", function() {
    let client: IPipeProcClient;
    let tcp: {host: string, port: number};

    beforeEach(async function(done) {
        client = PipeProc();
        tcp = {
            host: "127.0.0.1",
            port: await getRandomPort()
        };
        client.spawn({
            memory: true,
            workers: 0,
            tcp: tcp
        }).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function() {
        if (client.pipeProcNode) {
            return client.shutdown();
        } else {
            return Promise.resolve();
        }
    });

    it("should be able to connect to an existing node", function(done) {
        const client2 = PipeProc();
        client2.connect({tcp: tcp}).then(function(status) {
            expect(status).toEqual("connected");
            done();
        });
    });

    it("should allow multiple clients to connect", async function() {
        const client2 = PipeProc();
        const client3 = PipeProc();
        const status = await client2.connect({tcp: tcp});
        expect(status).toEqual("connected");
        const status2 = await client3.connect({tcp: tcp});
        expect(status2).toEqual("connected");
    });

    it("should return a notice if we are already connected", function(done) {
        client.connect({tcp: tcp}).then(function(status) {
            expect(status).toEqual("already_connected");
            done();
        });
    });
});

describe("shutting down a node with ipc namespace", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    it("should be able to shutdown a node", function(done) {
        //spawn it first
        client.spawn({memory: true, workers: 1, namespace: uuid()}).then(function() {
            client.shutdown().then(function(status) {
                expect(status).toEqual("closed");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should raise an error if there is no active node", function(done) {
        client.shutdown().catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("no_active_node");
            done();
        });
    });
});

describe("shutting down a node with ipc address", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    it("should be able to shutdown a node", function(done) {
        //spawn it first
        client.spawn({
            memory: true,
            workers: 1,
            socket: `ipc://${tmpdir()}/pipeproc.${uuid()}`
        }).then(function() {
            client.shutdown().then(function(status) {
                expect(status).toEqual("closed");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should raise an error if there is no active node", function(done) {
        client.shutdown().catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("no_active_node");
            done();
        });
    });
});

describe("shutting down a node with tcp address", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    it("should be able to shutdown a node", async function(done) {
        //spawn it first
        client.spawn({
            memory: true,
            workers: 1,
            socket: `tcp://127.0.0.1:${await getRandomPort()}`
        }).then(function() {
            client.shutdown().then(function(status) {
                expect(status).toEqual("closed");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should raise an error if there is no active node", function(done) {
        client.shutdown().catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("no_active_node");
            done();
        });
    });
});

describe("shutting down a node with tcp settings", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    it("should be able to shutdown a node", async function(done) {
        //spawn it first
        client.spawn({
            memory: true,
            workers: 1,
            tcp: {
                host: "127.0.0.1",
                port: await getRandomPort()
            }
        }).then(function() {
            client.shutdown().then(function(status) {
                expect(status).toEqual("closed");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should raise an error if there is no active node", function(done) {
        client.shutdown().catch(function(err) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("no_active_node");
            done();
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
