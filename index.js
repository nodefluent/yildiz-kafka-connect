"use strict";

const YildizSourceConfig = require("./lib/YildizSourceConfig.js");
const YildizSinkConfig = require("./lib/YildizSinkConfig.js");

const YildizSourceConnector = require("./lib/source/YildizSourceConnector.js");
const YildizSinkConnector = require("./lib/sink/YildizSinkConnector.js");

const YildizSourceTask = require("./lib/source/YildizSourceTask.js");
const YildizSinkTask = require("./lib/sink/YildizSinkTask.js");

const JsonConverter = require("./lib/utils/JsonConverter.js");
const ConverterFactory = require("./lib/utils/ConverterFactory.js");

const runSourceConnector = (properties, converters = [], onError = null) => {

    const config = new YildizSourceConfig(properties,
        YildizSourceConnector,
        YildizSourceTask, [JsonConverter].concat(converters));

    if (onError) {
        config.on("error", onError);
    }

    return config.run().then(() => {
        return config;
    });
};

const runSinkConnector = (properties, converters = [], onError = null) => {

    const config = new YildizSinkConfig(properties,
        YildizSinkConnector,
        YildizSinkTask, [JsonConverter].concat(converters));

    if (onError) {
        config.on("error", onError);
    }

    return config.run().then(() => {
        return config;
    });
};

module.exports = {
    runSourceConnector,
    runSinkConnector,
    ConverterFactory
};
