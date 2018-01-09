"use strict";

const {SinkConnector} = require("kafka-connect");
const {YildizClient} = require("yildiz-js");

class YildizSinkConnector extends SinkConnector {

    start(properties, callback) {

        this.properties = properties;

        const yildizOptions = {
            prefix: this.properties.prefix,
            proto: this.properties.proto,
            host: this.properties.host,
            port: this.properties.port
        };

        if (this.properties.token) {
            yildizOptions.token = this.properties.token;
        }

        const yildiz = new YildizClient(yildizOptions);

        this.properties.yildizClient = yildiz;

        yildiz.isAlive()
            .then(() => callback())
            .catch(error => callback(error));
    }

    taskConfigs(maxTasks, callback) {

        const taskConfig = {
            maxTasks,
            batchSize: this.properties.batchSize,
            yildizClient: this.properties.yildizClient,
            noTransaction: this.properties.noTransaction || false
        };

        callback(null, taskConfig);
    }

    stop() {
        // nothing to do
    }
}

module.exports = YildizSinkConnector;
