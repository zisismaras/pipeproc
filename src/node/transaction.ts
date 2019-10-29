import debug from "debug";
import {LevelDown as LevelDOWN, Bytes} from "leveldown";
import MemDOWN from "memdown";

const d = debug("pipeproc:node:transaction");
export interface ITransaction<T> {
    TRANSACTION: Symbol;
    _updates: {key: string, value: Bytes}[];
    _resultFns: {
        [key: string]: {
            fns: (() => T)[],
            position: number
        }
    };
    commitUpdate(
        callback: (
            err?: Error | null | undefined,
            ...result: (T | T[] | undefined)[]
        ) => void
    ): void;
    commitDelete(
        callback: (err?: Error) => void
    ): void;
    add(u: {
        key: string;
        value?: Bytes;
    } | {
        key: string;
        value?: Bytes;
    }[] | ITransaction<T> | ITransaction<T>[]): void;
    done(resultCb: () => T, resultBucket?: string): void;
}

export function transaction<T>(db: LevelDOWN): ITransaction<T> {
    d("starting new transaction...");
    return {
        TRANSACTION: Symbol("PIPEPROC_TRANSACTION"),
        _updates: [],
        _resultFns: {
            unamed_bucket: {
                fns: [],
                position: 1
            }
        },
        commitUpdate: function(callback) {
            if (this._updates.length === 0) return callback();
            d("commiting update...");
            const updates = this._updates.map(u => {
                //tslint:disable no-object-literal-type-assertion prefer-type-cast
                return {type: "put", key: u.key, value: u.value} as {type: "put", key: string, value: string};
                //tslint: enable
            });
            if (this._resultFns.unamed_bucket.fns.length === 0) delete this._resultFns.unamed_bucket;
            const results = Object.keys(this._resultFns)
            .map(bucket => {
                const bucketContent = this._resultFns[bucket];
                return {
                    fns: bucketContent.fns.map(re => re()),
                    position: bucketContent.position
                };
            });
            results.sort((a, b) => {
                if (a.position < b.position) {
                    return -1;
                } else if ((a.position > b.position)) {
                    return 1;
                } else {
                    return 0;
                }
            });
            if (db instanceof MemDOWN) {
                db.batch(updates, commitError => {
                    if (commitError) {
                        callback(commitError);
                    } else {
                        callback(null, ...(results.map(sortedBucketContent => {
                            if (sortedBucketContent.fns.length > 1) {
                                return sortedBucketContent.fns;
                            } else {
                                return sortedBucketContent.fns[0];
                            }
                        })));
                    }
                });
            } else {
                db.batch(updates, {sync: true}, commitError => {
                    if (commitError) {
                        callback(commitError);
                    } else {
                        callback(null, ...(results.map(sortedBucketContent => {
                            if (sortedBucketContent.fns.length > 1) {
                                return sortedBucketContent.fns;
                            } else {
                                return sortedBucketContent.fns[0];
                            }
                        })));
                    }
                });
            }
        },
        commitDelete: function(callback) {
            if (this._updates.length === 0) return callback();
            d("commiting delete...");
            const updates = this._updates.map(u => {
                //tslint:disable no-object-literal-type-assertion prefer-type-cast
                return {type: "del", key: u.key} as {type: "del", key: string};
                //tslint: enable
            });
            if (db instanceof MemDOWN) {
                db.batch(updates, function(rollbackError) {
                    if (rollbackError) {
                        callback(rollbackError);
                    } else {
                        callback();
                    }
                });
            } else {
                db.batch(updates, {sync: true}, function(rollbackError) {
                    if (rollbackError) {
                        callback(rollbackError);
                    } else {
                        callback();
                    }
                });
            }
        },
        add: function(u) {
            d("adding operations...");
            if (Array.isArray(u)) {
                (<({key: string, value: string} | ITransaction<T>)[]>u).forEach((uu) => {
                        if ((<ITransaction<T>>uu).TRANSACTION) {
                            this._updates = this._updates.concat((<ITransaction<T>>uu)._updates);
                            Object.keys((<ITransaction<T>>uu)._resultFns).forEach(bucket => {
                                if (this._resultFns[bucket]) {
                                    this._resultFns[bucket].fns = this._resultFns[bucket].fns.concat(
                                        (<ITransaction<T>>uu)._resultFns[bucket].fns
                                    );
                                } else {
                                    this._resultFns[bucket] = (<ITransaction<T>>uu)._resultFns[bucket];
                                }
                            });
                        } else {
                            this._updates.push(<{key: string, value: string}>uu);
                        }
                    }
                );
            } else {
                if ((<ITransaction<T>>u).TRANSACTION) {
                    this._updates = this._updates.concat((<ITransaction<T>>u)._updates);
                    Object.keys((<ITransaction<T>>u)._resultFns).forEach(bucket => {
                        if (this._resultFns[bucket]) {
                            this._resultFns[bucket].fns = this._resultFns[bucket].fns.concat(
                                (<ITransaction<T>>u)._resultFns[bucket].fns
                            );
                        } else {
                            this._resultFns[bucket] = (<ITransaction<T>>u)._resultFns[bucket];
                        }
                    });
                } else {
                    this._updates.push(<{key: string, value: string}>u);
                }
            }
        },
        done: function(resultCb, resultBucket) {
            d("tracking results...");
            let bucketName;
            if (!resultBucket) {
                bucketName = "unamed_bucket";
            } else {
                bucketName = resultBucket;
            }
            this._resultFns[bucketName] = this._resultFns[bucketName] ||
                {fns: [], position: Object.keys(this._resultFns).length + 1};
            this._resultFns[bucketName].fns.push(resultCb);
        }
    };
}
