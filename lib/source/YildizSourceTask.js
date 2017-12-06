"use strict";

const { SourceTask, SourceRecord } = require("kafka-connect");

class YildizSourceTask extends SourceTask {

    start(properties, callback, parentConfig) {

        this.parentConfig = parentConfig;

        this.properties = properties;
        const {
            yildizClient,
            maxTasks,
            maxPollCount
        } = this.properties;

        this.yildizClient = yildizClient;
        this.maxTasks = maxTasks;
        this.maxPollCount = maxPollCount;

        this._stats = {
            totalPulls: 0,
            messagesPulled: 0,
            messagesAcked: 0,
            pullErrors: 0
        }

        this.parentConfig.on("get-stats", () => {
            this.parentConfig.emit("any-stats", "yildiz-source", this._stats);
        });

        callback(null);
    }

    poll(callback) {
        callback(null, []);
    }

    stop() {
        //empty
    }
}

module.exports = YildizSourceTask;
