//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";

describe("spawning the node", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    afterEach(function(done) {
        if (client.pipeProcNode) {
            client.shutdown(function(err) {
                if (err) return done.fail(err);
                done();
            });
        } else {
            done();
        }
    });

    it("should spawn correctly", function(done) {
        client.spawn({memory: true, workers: 0, namespace: uuid()}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should return a warning if spawned twice", function(done) {
        (<Promise<string>>client.spawn({memory: true, workers: 0, namespace: uuid()})).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            (<Promise<string>>client.spawn({memory: true})).then(function(status2) {
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

describe("connecting to an existing node", function() {
    let client: IPipeProcClient;
    let namespace: string;

    beforeEach(function(done) {
        client = PipeProc();
        namespace = uuid();
        (<Promise<string>>client.spawn({memory: true, workers: 0, namespace: namespace})).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function(done) {
        if (client.pipeProcNode) {
            client.shutdown(function(err) {
                if (err) return done.fail(err);
                done();
            });
        } else {
            done();
        }
    });

    it("should be able to connect to an existing node", function(done) {
        const client2 = PipeProc();
        client2.connect({namespace: namespace}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("connected");
            done();
        });
    });

    it("should allow multiple clients to connect", function(done) {
        const client2 = PipeProc();
        const client3 = PipeProc();
        client2.connect({namespace: namespace}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("connected");
            client3.connect({namespace: namespace}, function(err2, status2) {
                expect(err2).toBeNull();
                expect(status2).toEqual("connected");
                done();
            });
        });
    });

    it("should return a notice if we are already connected", function(done) {
        client.connect({namespace: namespace}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("already_connected");
            done();
        });
    });
});

describe("shutting down a node", function() {
    let client: IPipeProcClient;

    beforeEach(function() {
        client = PipeProc();
    });

    it("should be able to shutdown a node", function(done) {
        //spawn it first
        (<Promise<string>>client.spawn({memory: true, workers: 0, namespace: uuid()})).then(function() {
            client.shutdown(function(err, status) {
                expect(err).toBeNull();
                expect(status).toEqual("closed");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should raise an error if there is no active node", function(done) {
        client.shutdown(function(err, status) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("no_active_node");
            expect(status).toBeUndefined();
            done();
        });
    });
});
