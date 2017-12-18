"use strict";

const async = require("async");

const { SourceConnector } = require("kafka-connect");
const { YildizClient } = require("yildiz-js");

class YildizSourceConnector extends SourceConnector {

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
