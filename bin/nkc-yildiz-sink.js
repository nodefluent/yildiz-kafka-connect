#!/usr/bin/env node

const program = require("commander");
const path = require("path");
const { runSinkConnector } = require("./../index.js");
const pjson = require("./../package.json");
const loadConfig = require("./../config/loadConfig.js");

program
    .version(pjson.version)
    .option("-c, --config [string]", "Path to Config (optional)")
    .option("-k, --kafka [string]", "Zookeeper Connection String")
    .option("-g, --group [string]", "Kafka ConsumerGroup Id")
    .option("-t, --topic [string]", "Kafka Topic to read from")
    .option("-x, --prefix [string]", "yildiz db prefix")
    .option("-p, --proto [string]", "Protocol used for yildiz connection")
    .option("-h, --host [string]", "Host string for yildiz db server")
    .option("-r, --port [number]", "Port for yildiz connection")
    .option("-e, --token [string]", "(Optional) Token for yildiz connection")
    .option("-o, --batch_size [integer]", "Batch size for inserts")
    .parse(process.argv);

const config = loadConfig(program.config);

if (program.kafka) {
    config.kafka.zkConStr = program.kafka;
}

if (program.name) {
    config.kafka.clientName = program.name;
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

if (program.batch_size) {
    config.connector.batchSize = program.batch_size;
}

runSinkConnector(config, [], console.log.bind(console)).then(sink => {

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
