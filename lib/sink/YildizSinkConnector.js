"use strict";

const { SinkConnector } = require("kafka-connect");
const { YildizClient } = require("yildiz-js");

class YildizSinkConnector extends SinkConnector {

    start(properties, callback) {

        this.properties = properties;

        const yildiz = new YildizClient({
            prefix: this.properties.prefix,
            proto: this.properties.proto,
            host: this.properties.host,
            port: this.properties.port
        });

        this.properties.yildizClient = yildiz;

        yildiz.isAlive()
            .then(() => callback())
            .catch(error => callback(error));
    }

    taskConfigs(maxTasks, callback) {

        const taskConfig = {
            maxTasks,
            batchSize: this.properties.batchSize,
            yildizClient: this.properties.yildizClient
        };

        callback(null, taskConfig);
    }

    stop() {
        // nothing to do
    }
}

module.exports = YildizSinkConnector;
