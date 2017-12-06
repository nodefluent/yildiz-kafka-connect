"use strict";

const { SinkConfig } = require("kafka-connect");

class YildizSinkConfig extends SinkConfig {

    constructor(...args) { super(...args); }

    run() {
        return super.run();
    }
}

module.exports = YildizSinkConfig;
