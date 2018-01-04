"use strict";

const async = require("async");
const { SinkTask } = require("kafka-connect");

class YildizSinkTask extends SinkTask {

    start(properties, callback, parentConfig) {

        this.parentConfig = parentConfig;
        this.properties = properties;
        const {
            yildizClient,
            maxTasks,
            batchSize
        } = this.properties;

        this.yildizClient = yildizClient;
        this.batchSize = batchSize;
        this.maxTasks = maxTasks;

        this.buffer = [];
        this.bufferDraining = false;

        this._stats = {
            nodesCreated: 0,
            transalationsCreated: 0,
            nodesFound: 0,
            edgesCreated: 0,
            edgeDepthsIncreased: 0
        };

        this.parentConfig.on("get-stats", () => {
            this.parentConfig.emit("any-stats", "yildiz-sink", this._stats);
        });

        callback();
    }

    upsertRelations(relations) {
        return new Promise((resolve, reject) => {
            async.eachSeries(
                relations,
                (relation, next) => {
                    upsertRelation(relation)
                        .then(() => next())
                        .catch(error => next(error));
                },
                error => {
                    if (error) {
                        return reject(error);
                    }

                    resolve();
                }
            )
        });
    }

    upsertRelation(relation) {

        const {
            leftNodeIdentifierVal,
            rightNodeIdentifierVal,
            leftNodeData = {},
            rightNodeData = {},
            ttld = false,
            relation = "1",
            edgeData = {},
            depthBeforeCreation = true
        } = relation;

        return this.yildizClient.upsertRelation(
            leftNodeIdentifierVal,
            rightNodeIdentifierVal,
            leftNodeData,
            rightNodeData,
            ttld,
            relation,
            edgeData,
            depthBeforeCreation
        )
            .then(() => this.parentConfig.emit("model-upsert"));
    }

    /**
     * expects an array of relation objects with the following structure
     * [{
     *     leftNodeIdentifierVal: string,
     *     rightNodeIdentifierVal: string,
     *     leftNodeData: object, 
     *     rightNodeData: object,
     *     ttld: boolean, 
     *     relation: string, 
     *     edgeData: object, 
     *     depthBeforeCreation: boolean
     * }]
     */
    async putRecords(records) {
        return new Promise((resolve, reject) => {
            async.eachSeries(
                records,
                (record, next) => {
                    if (!record.value) {
                        return next();
                    }
                    this.upsertRelations(record.value)
                        .then(() => next())
                        .catch(error => next(error));
                },
                error => {
                    if (error) {
                        return reject(error);
                    }

                    resolve();
                }
            )
        });
    }

    put(records, callback) {
        this.putRecords(records)
            .then(() => callback(null))
            .catch(error => callback(error));
    }

    stop() {
        //empty
    }
}

module.exports = YildizSinkTask;
