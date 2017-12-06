"use strict";

const { SourceConfig } = require("kafka-connect");

class YildizSourceConfig extends SourceConfig {

    constructor(...args) { super(...args); }

    run() {
        return super.run();
    }
}

module.exports = YildizSourceConfig;
