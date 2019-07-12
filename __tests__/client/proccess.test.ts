//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {getRandomPort} from "../utils/getRandomPort";

describe("spawning the node", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    afterEach(async function() {
        try {
            await client.shutdown();
        } catch (_e) {}
    });

    it("should spawn correctly", async function() {
        const status = await client.spawn({
            memory: true,
            workers: 0,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        });
        expect(status).toEqual("spawned_and_connected");
    });

    it("should return a warning if spawned twice", async function() {
        const status = await client.spawn({
            memory: true,
            workers: 0,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        });
        expect(status).toEqual("spawned_and_connected");
        const status2 = await client.spawn({
            memory: true,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        });
        expect(status2).toEqual("node_already_active");
    });
});

describe("connecting to an existing node", function() {
    let client: IPipeProcClient;
    let tcp;

    beforeEach(async function() {
        client = PipeProc();
        tcp = {host: "127.0.0.1", port: await getRandomPort()};
        return client.spawn({memory: true, workers: 0, tcp: tcp});
    });

    afterEach(async function() {
        await client.shutdown();
    });

    it("should be able to connect to an existing node", async function() {
        const client2 = PipeProc();
        const status = await client2.connect({tcp: tcp});
        expect(status).toEqual("connected");
        await client2.shutdown();
    });

    it("should allow multiple clients to connect", async function() {
        const client2 = PipeProc();
        const client3 = PipeProc();
        const status = await client2.connect({tcp: tcp});
        expect(status).toEqual("connected");
        const status2 = await client3.connect({tcp: tcp});
        expect(status2).toEqual("connected");
        await client2.shutdown();
        await client3.shutdown();
    });

    it("should return a notice if we are already connected", async function() {
        const status = await client.connect({tcp: tcp});
        expect(status).toEqual("already_connected");
    });
});

describe("shutting down a node", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    it("should be able to shutdown a node", async function() {
        //spawn it first
        await client.spawn({
            memory: true,
            workers: 1,
            tcp: {host: "127.0.0.1", port: await getRandomPort()}
        });
        const status = await client.shutdown();
        expect(status).toEqual("closed");
    });

    it("should raise an error if there is no active node", async function() {
        expect((async function() {
            //@ts-ignore
            await client.shutdown();
        })()).rejects.toThrowError("no_active_node");
    });
});
