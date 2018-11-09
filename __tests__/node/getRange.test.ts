//tslint:disable
import "jest-extended";
//tslint:enable
import LevelDOWN from "leveldown";
import MemDOWN from "memdown";
import {eachSeries} from "async";
import {commitLog} from "../../src/node/commitLog";
import {getRange} from "../../src/node/getRange";
import {IActiveTopics} from "../../src/node/pipeProc";

describe("get logs by full log id", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let addedIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
                return done.fail((err && err.message) || "uknown_error");
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                } else {
                    return done.fail("Invalid logs created");
                }
            }
            done();
        });
    });
    test("it should return all the logs when no range is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            expect(results).toHaveLength(addedIds.length);
            done();
        });
    });
    test("the results should contain the correct body values", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            if (Array.isArray(results)) {
                results.forEach((re, i) => {
                    expect(re.data).toBe(`{"myData": ${i + 1}}`);
                });
            } else {
                return done.fail("No results found");
            }
            done();
        });
    });
    test("it should return only specific logs if a 'start' is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[2],
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(4);
            expect(results[0].id).toBe(addedIds[2]);
            expect(results[3].id).toBe(addedIds[5]);
            done();
        });
    });
    test("it should return only specific logs if an 'end' is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            addedIds[2],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[0]);
            expect(results[2].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("it should return only specific logs if a 'start' and an 'end' are provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[2],
            addedIds[4],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[2]);
            expect(results[2].id).toEqual(addedIds[4]);
            done();
        });
    });
    test("it should honor the limit option", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[2],
            addedIds[4],
            1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(1);
            expect(results[0].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("'exclusive' should not include the upper and lower range limits", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[2],
            addedIds[4],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(1);
            expect(results[0].id).toEqual(addedIds[3]);
            done();
        });
    });
    test("'reverse' should fetch results in reversed order and swap 'start' and 'end' parameters", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[4],
            addedIds[2],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[4]);
            expect(results[2].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[4],
            addedIds[0],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'exclusive' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[4],
            addedIds[0],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'reverse' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[0],
            addedIds[4],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'reverse' and 'exclusive' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            addedIds[0],
            addedIds[4],
            -1,
            true,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("using an invalid topic should return an error", function(done) {
        getRange(
            db,
            activeTopics,
            "my_invalid_topic",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            expect(err).toBeInstanceOf(Error);
            expect(results).toBeUndefined();
            done();
        });
    });
});

describe("get logs by timestamp only", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let addedIds: string[];
    let timestamps: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        addLogsToTopicWithDelay(db, activeTopics, function(err, ids) {
            if (err) {
              return done.fail((err && err.message) || "uknown_error");
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    timestamps = ids.map(id => id.split("-")[0]);
                } else {
                  return done.fail("Invalid logs created");
                }
            }
            done();
        });
    });
    test("it should return all the logs when no range is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            expect(results).toHaveLength(addedIds.length);
            done();
        });
    });
    test("the results should contain the correct body values", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            if (Array.isArray(results)) {
                results.forEach((re, i) => {
                    expect(re.data).toBe(`{"myData": ${i + 1}}`);
                });
            } else {
                return done.fail("No results found");
            }
            done();
        });
    });
    test("it should return only specific logs if a 'start' is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(4);
            expect(results[0].id).toBe(addedIds[2]);
            expect(results[3].id).toBe(addedIds[5]);
            done();
        });
    });
    test("it should return only specific logs if an 'end' is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            timestamps[2],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[0]);
            expect(results[2].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("it should return only specific logs if a 'start' and an 'end' are provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            timestamps[4],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[2]);
            expect(results[2].id).toEqual(addedIds[4]);
            done();
        });
    });
    test("it should honor the limit option", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            timestamps[4],
            1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(1);
            expect(results[0].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("even with 'exclusive', upper and lower limits will still be returned", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            timestamps[4],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[2]);
            expect(results[2].id).toEqual(addedIds[4]);
            done();
        });
    });
    test("'reverse' should fetch results in reversed order and swap 'start' and 'end' parameters", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[4],
            timestamps[2],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[4]);
            expect(results[2].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[4],
            timestamps[0],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'exclusive' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[4],
            timestamps[0],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'reverse' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[0],
            timestamps[4],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'reverse' and 'exclusive' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[0],
            timestamps[4],
            -1,
            true,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("using an invalid topic should return no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_invalid_topic",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            expect(err).toBeInstanceOf(Error);
            expect(results).toBeUndefined();
            done();
        });
    });
});

describe("get logs by timestamp only (using the same timestamp)", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let addedIds: string[];
    let timestamps: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
              return done.fail((err && err.message) || "uknown_error");
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    timestamps = ids.map(id => id.split("-")[0]);
                } else {
                  return done.fail("Invalid logs created");
                }
            }
            done();
        });
    });
    test("it should return all the logs when no range is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            expect(results).toHaveLength(addedIds.length);
            done();
        });
    });
    test("the results should contain the correct body values", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            if (Array.isArray(results)) {
                results.forEach((re, i) => {
                    expect(re.data).toBe(`{"myData": ${i + 1}}`);
                });
            } else {
                return done.fail("No results found");
            }
            done();
        });
    });
    test("it should ignore the 'start'", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(6);
            expect(results[0].id).toBe(addedIds[0]);
            expect(results[5].id).toBe(addedIds[5]);
            done();
        });
    });
    test("it should ignore the 'end'", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            timestamps[2],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(6);
            expect(results[0].id).toBe(addedIds[0]);
            expect(results[5].id).toBe(addedIds[5]);
            done();
        });
    });
    test("it should ignore 'start' and 'end'", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            timestamps[4],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(6);
            expect(results[0].id).toBe(addedIds[0]);
            expect(results[5].id).toBe(addedIds[5]);
            done();
        });
    });
    test("it should honor the limit option", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            timestamps[4],
            1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(1);
            done();
        });
    });
    test("'exclusive' should have no effect", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[2],
            timestamps[4],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(6);
            expect(results[0].id).toEqual(addedIds[0]);
            expect(results[5].id).toEqual(addedIds[5]);
            done();
        });
    });
    test("'reverse' should fetch results in reversed order", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            timestamps[4],
            timestamps[2],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(6);
            expect(results[0].id).toEqual(addedIds[5]);
            expect(results[5].id).toEqual(addedIds[0]);
            done();
        });
    });
    test("using an invalid topic should return no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_invalid_topic",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            expect(err).toBeInstanceOf(Error);
            expect(results).toBeUndefined();
            done();
        });
    });
});

describe("get logs by tone id only", function() {
    let db: LevelDOWN.LevelDown;
    let activeTopics: IActiveTopics;
    let addedIds: string[];
    let toneIds: string[];
    beforeEach(function(done) {
        db = MemDOWN();
        activeTopics = {};
        addLogsToTopic(db, activeTopics, function(err, ids) {
            if (err) {
              return done.fail((err && err.message) || "uknown_error");
            } else {
                if (ids && Array.isArray(ids)) {
                    addedIds = ids;
                    toneIds = ids.map(id => `:${id.split("-")[1]}`);
                } else {
                  return done.fail("Invalid logs created");
                }
            }
            done();
        });
    });
    test("it should return all the logs when no range is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            expect(results).toHaveLength(addedIds.length);
            done();
        });
    });
    test("the results should contain the correct body values", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            expect(results).toBeArray();
            if (Array.isArray(results)) {
                results.forEach((re, i) => {
                    expect(re.data).toBe(`{"myData": ${i + 1}}`);
                });
            } else {
                return done.fail("No results found");
            }
            done();
        });
    });
    test("it should return only specific logs if a 'start' is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[2],
            "",
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(4);
            expect(results[0].id).toBe(addedIds[2]);
            expect(results[3].id).toBe(addedIds[5]);
            done();
        });
    });
    test("it should return only specific logs if an 'end' is provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            toneIds[2],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[0]);
            expect(results[2].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("it should return only specific logs if a 'start' and an 'end' are provided", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[2],
            toneIds[4],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[2]);
            expect(results[2].id).toEqual(addedIds[4]);
            done();
        });
    });
    test("it should honor the limit option", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[2],
            toneIds[4],
            1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(1);
            expect(results[0].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("'exclusive' should not include the upper and lower range limits", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[2],
            toneIds[4],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(1);
            expect(results[0].id).toEqual(addedIds[3]);
            done();
        });
    });
    test("'reverse' should fetch results in reversed order and swap 'start' and 'end' parameters", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[4],
            toneIds[2],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(3);
            expect(results[0].id).toEqual(addedIds[4]);
            expect(results[2].id).toEqual(addedIds[2]);
            done();
        });
    });
    test("an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[4],
            toneIds[0],
            -1,
            false,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'exclusive' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[4],
            toneIds[0],
            -1,
            true,
            false,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'reverse' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[0],
            toneIds[4],
            -1,
            false,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("'reverse' and 'exclusive' with an incorrent range should fetch no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            toneIds[0],
            toneIds[4],
            -1,
            true,
            true,
        function(err, results) {
            if (err) return done.fail((err && err.message) || "uknown_error");
            if (!results) return done.fail("No results found");
            expect(results).toBeArray();
            expect(results).toHaveLength(0);
            done();
        });
    });
    test("using an invalid topic should return no results", function(done) {
        getRange(
            db,
            activeTopics,
            "my_invalid_topic",
            "",
            "",
            -1,
            false,
            false,
        function(err, results) {
            expect(err).toBeInstanceOf(Error);
            expect(results).toBeUndefined();
            done();
        });
    });
    test("using invalid tone id as 'start' should return an error", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            ":10",
            "",
            -1,
            false,
            false,
        function(err, results) {
            expect(err).toBeInstanceOf(Error);
            expect(results).toBeUndefined();
            done();
        });
    });
    test("using invalid tone id as 'end' should return an error", function(done) {
        getRange(
            db,
            activeTopics,
            "my_topic_0",
            "",
            ":15",
            -1,
            false,
            false,
        function(err, results) {
            expect(err).toBeInstanceOf(Error);
            expect(results).toBeUndefined();
            done();
        });
    });
});

function addLogsToTopic(
    db: LevelDOWN.LevelDown,
    activeTopics: IActiveTopics,
    callback: (err: Error|null, ids?: string|string[]) => void)
: void {
    commitLog(db, activeTopics, [{
        topic: "my_topic_0",
        body: "{\"myData\": 1}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 2}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 3}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 4}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 5}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 6}"
    }], callback);
}

function addLogsToTopicWithDelay(
    db: LevelDOWN.LevelDown,
    activeTopics: IActiveTopics,
    callback: (err: Error|null, ids?: string|string[]) => void)
: void {
    const logs = [{
        topic: "my_topic_0",
        body: "{\"myData\": 1}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 2}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 3}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 4}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 5}"
    }, {
        topic: "my_topic_0",
        body: "{\"myData\": 6}"
    }];
    const ids: string[] = [];
    eachSeries(logs, function(log, next) {
        commitLog(db, activeTopics, log, function(_, id) {
            setTimeout(function() {
                if (typeof id === "string") {
                    ids.push(id);
                }
                next();
            }, 20);
        });
    }, function() {
        callback(null, ids);
    });
}
