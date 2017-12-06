"use strict";

const { Converter } = require("kafka-connect");

class InjectableConverter extends Converter {

    constructor(etl) {
        super();
        this.etl = etl;
    }

    fromConnectData(data, callback) {
        callback(null, data); //no action required, as we produce objects directly
    }

    toConnectData(message, callback) {
        this.etl(message, callback);
    }
}

class ConverterFactory {

    /**
     * Pass in the schema description of the yildiz message object
     * and a function that receives the message value
     * calls a callback(null, {}) with the transformed value
     * in form of the message that is written to yildiz.
     * returns an instance of a Converter that can be passed into the
     * Converter-Array param of the SinkConfig
     * @param {*} yildizSchema
     * @param {*} etlFunction
     * @return {}
     */
    static createSinkSchemaConverter(yildizSchema, etlFunction) {

        if (typeof yildizSchema !== "object") {
            throw new Error("yildizSchema must be an object.");
        }

        if (typeof etlFunction !== "function") {
            throw new Error("etlFunction must be a function.");
        }

        return new InjectableConverter(ConverterFactory._getSinkSchemaETL(yildizSchema, etlFunction));
    }

    static _getSinkSchemaETL(yildizSchema, etlFunction) {

        const schema = yildizSchema;
        const etl = etlFunction;

        return (message, callback) => {

            etlFunction(message.value, (error, messageValue) => {

                if (error) {
                    return callback(error);
                }

                message.value = {
                    key: message.key,
                    keySchema: null,
                    value: messageValue,
                    valueSchema: Object.assign({}, schema),
                    partition: message.partition,
                    timestamp: new Date().toISOString(),
                    topic: message.topic
                };

                callback(null, message);
            });
        };
    }
}

module.exports = ConverterFactory;
