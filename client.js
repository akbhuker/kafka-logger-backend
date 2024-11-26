const { Kafka } = require('kafkajs');

exports.kafka = new Kafka({
    clientId: 'log-producer',
    brokers: ['192.168.31.107:9092']
});
