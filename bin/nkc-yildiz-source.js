#!/usr/bin/env node

const program = require("commander");
const path = require("path");
const { runSourceConnector } = require("./../index.js");
const pjson = require("./../package.json");
const loadConfig = require("./../config/loadConfig.js");

program
    .version(pjson.version)
    .option("-c, --config [string]", "Path to Config (alternatively)")
    .option("-k, --kafka [string]", "Zookeeper Connection String")
    .option("-n, --client_name [string]", "Kafka Client Name")
    .option("-t, --topic [string]", "Kafka Topic to Produce to")
    .option("-a, --partitions [integer]", "Amount of Kafka Topic Partitions")
    .option("-x, --prefix [string]", "yildiz db prefix")
    .option("-p, --proto [string]", "Protocol used for yildiz connection")
    .option("-h, --host [string]", "Host string for yildiz db server")
    .option("-r, --port [number]", "Port for yildiz connection")
    .option("-e, --token [string]", "(Optional) Token for yildiz connection")
    .option("-v, --interval [integer]", "Table poll interval (ms)")
    .option("-o, --max_pollcount [integer]", "Max row count per poll action")
    .parse(process.argv);

const config = loadConfig(program.config);

if (program.kafka) {
    config.kafka.zkConStr = program.kafka;
}

if (program.client_name) {
    config.kafka.clientName = program.client_name;
}

if (program.topic) {
    config.topic = program.topic;
}

if (program.proto) {
    config.proto = program.proto;
}

if (program.host) {
    config.host = program.host;
}

if (program.port) {
    config.port = program.port;
}

if (program.token) {
    config.token = program.token;
}

if (program.partitions) {
    config.partitions = program.partitions;
}

if (program.interval) {
    config.pollInterval = program.interval;
}

if (program.max_pollcount) {
    config.connector.maxPollCount = program.max_pollcount;
}

runSourceConnector(config, [], console.log.bind(console)).then(sink => {

    const exit = (isExit = false) => {
        sink.stop();
        if (!isExit) {
            process.exit();
        }
    };

    process.on("SIGINT", () => {
        exit(false);
    });

    process.on("exit", () => {
        exit(true);
    });
});
