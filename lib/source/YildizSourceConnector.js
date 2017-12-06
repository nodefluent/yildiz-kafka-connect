"use strict";

const async = require("async");

const { SourceConnector } = require("kafka-connect");
const { YildizClient } = require("yildiz-js");

class YildizSourceConnector extends SourceConnector {

    start(properties, callback) {

        this.properties = properties;

        const yildiz = new YildizClient({
            proto: this.properties.proto,
            host: this.properties.host,
            port: this.properties.port
        });

        yildiz.isAlive()
            .then(() => callback())
            .catch(error => callback(error));

        this.properties.yildizClient = yildiz;
    }

    taskConfigs(maxTasks, callback) {

        const taskConfig = {
            maxTasks,
            maxPollCount: this.properties.maxPollCount,
            yildizClient: this.properties.yildizClient
        };

        callback(null, taskConfig);
    }

    stop() {
        //empty
    }
}

module.exports = YildizSourceConnector;
