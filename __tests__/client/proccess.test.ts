//tslint:disable
import "jest-extended";
//tslint:enable
import {pipeProcClient} from "../../lib/client";

describe("spawning the node", function() {

    afterEach(function(done) {
        if (pipeProcClient.pipeProcNode) {
            pipeProcClient.shutdown(function(err) {
                if (err) return done.fail(err);
                done();
            });
        } else {
            done();
        }
    });

    it("should spawn correctly", function(done) {
        pipeProcClient.spawn({memory: true}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("spawned_and_connected");
            done();
        });
    });

    it("should return an error if spawned twice", function(done) {
        pipeProcClient.spawn({memory: true}).then(function(status) {
            expect(status).toEqual("spawned_and_connected");
            pipeProcClient.spawn({memory: true}).then(function() {
                done.fail(new Error("spawing twice should have raised an error"));
            }).catch(function(err) {
                expect(err).toBeInstanceOf(Error);
                expect(err.message).toEqual("node_already_active");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });
});

describe("connecting to an existing node", function() {

    beforeEach(function(done) {
        pipeProcClient.spawn({memory: true}).then(function() {
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    afterEach(function(done) {
        if (pipeProcClient.pipeProcNode) {
            pipeProcClient.shutdown(function(err) {
                if (err) return done.fail(err);
                done();
            });
        } else {
            done();
        }
    });

    it("should be able to connect to an existing node", function(done) {
        //simulate a not yet connected client
        pipeProcClient.ipc.disconnect("pipeproc");
        delete pipeProcClient.ipc;
        pipeProcClient.connect({}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("connected");
            done();
        });
    });

    it("should return a notice if we are already connected", function(done) {
        pipeProcClient.connect({}, function(err, status) {
            expect(err).toBeNull();
            expect(status).toEqual("already_connected");
            done();
        });
    });
});

describe("shutting down a node", function() {

    it("should be able to shutdown a node", function(done) {
        //spawn it first
        pipeProcClient.spawn({memory: true}).then(function() {
            pipeProcClient.shutdown(function(err, status) {
                expect(err).toBeNull();
                expect(status).toEqual("closed");
                done();
            });
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should raise an error if there is no active node", function(done) {
        pipeProcClient.shutdown(function(err, status) {
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toEqual("no_active_node");
            expect(status).toBeUndefined();
            done();
        });
    });
});
