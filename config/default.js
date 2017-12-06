"use strict";

const path = require("path");

const config = {
    kafka: {
        kafkaHost: "localhost:9092",
        groupId: "kc-yildiz-test",
        clientName: "kc-yildiz-test-name",
        workerPerPartition: 1,
        options: {
            sessionTimeout: 8000,
            protocol: ["roundrobin"],
            fromOffset: "earliest", //latest
            fetchMaxBytes: 1024 * 100,
            fetchMinBytes: 1,
            fetchMaxWaitMs: 10,
            heartbeatInterval: 250,
            retryMinTimeout: 250,
            requireAcks: 0,
            //ackTimeoutMs: 100,
            //partitionerType: 3
        }
    },
    topic: "yildiz-topic",
    partitions: 1,
    maxTasks: 1,
    pollInterval: 250,
    produceKeyed: true,
    produceCompressionType: 0,
    connector: {
        batchSize: 100,
        maxPollCount: 100,
        prefix: "ykc",
        proto: "http",
        host: "127.0.0.1",
        port: 3058
    },
    http: {
        port: 3149,
        middlewares: []
    },
    enableMetrics: true
};

module.exports = config;
