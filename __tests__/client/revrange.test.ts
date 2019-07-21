//tslint:disable
import "jest-extended";
//tslint:enable
import {PipeProc, IPipeProcClient} from "../../lib/client";
import {v4 as uuid} from "uuid";

//since revrange is just some sugar on range with {reverse: true} we only test some start/end cases

describe("using revrange", function() {
    let testLogIds: string[];
    let client: IPipeProcClient;

    beforeEach(function(done) {
        client = PipeProc();

        client.spawn({memory: true, workers: 0, namespace: uuid()}).then(function() {
            commitSomeLogs(client).then(function(logs) {
                testLogIds = logs;
                done();
            }).catch(function(err) {
                done.fail(err);
            });
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

    it("should not return results if start < end", function(done) {
        client.revrange("my_topic", {
            start: testLogIds[0],
            end: testLogIds[1]
        }).then(function(logs) {
            expect(logs).toBeArrayOfSize(0);
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

    it("should return the results in a reversed order", function(done) {
        client.revrange("my_topic", {
            start: testLogIds[2]
        }).then(function(logs) {
            expect(logs).toBeArrayOfSize(3);
            expect(logs[0].id).toEqual(testLogIds[2]);
            expect(logs[1].id).toEqual(testLogIds[1]);
            expect(logs[2].id).toEqual(testLogIds[0]);
            done();
        }).catch(function(err) {
            done.fail(err);
        });
    });

});

function commitSomeLogs(client) {
    return client.commit([{
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
    }]);
}
