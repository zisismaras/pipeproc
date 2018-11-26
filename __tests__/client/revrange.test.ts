//tslint:disable
import "jest-extended";
//tslint:enable
import {pipeProcClient} from "../../lib/client";

//since revrange is just some sugar on range with {reverse: true} we only test some start/end cases

describe("using revrange", function() {
    let testLogIds: string[];

    beforeEach(function(done) {
        (<Promise<string>>pipeProcClient.spawn({memory: true})).then(function() {
            commitSomeLogs().then(function(logs) {
                testLogIds = logs;
                done();
            }).catch(function(err) {
                done.fail(err);
            });
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

    it("should not return results if start < end", function(done) {
        pipeProcClient.revrange("my_topic", {
            start: testLogIds[0],
            end: testLogIds[1]
        }, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(0);
            done();
        });
    });

    it("should return the results in a reversed order", function(done) {
        pipeProcClient.revrange("my_topic", {
            start: testLogIds[2]
        }, function(err, logs) {
            expect(err).toBeNull();
            expect(logs).toBeArrayOfSize(3);
            expect(logs[0].id).toEqual(testLogIds[2]);
            expect(logs[1].id).toEqual(testLogIds[1]);
            expect(logs[2].id).toEqual(testLogIds[0]);
            done();
        });
    });

});

function commitSomeLogs() {
    return (<Promise<string[]>>pipeProcClient.commit([{
        topic: "my_topic",
        body: {
            hello: 1
        }
    }, {
        topic: "my_topic",
        body: {
            hello: 1
        }
    }, {
        topic: "my_topic",
        body: {
            hello: 1
        }
    }]));
}
