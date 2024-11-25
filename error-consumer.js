const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'error-log-consumer',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'error-monitoring-group' });

async function runConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'logs', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const logData = JSON.parse(message.value.toString());

            // Only process error logs
            if (logData.level === 'error') {
                console.log('Error Log Detected:');
                console.log('Service:', logData.service);
                console.log('Message:', logData.message);
                console.log('Timestamp:', logData.timestamp);
                console.log('-------------------');

                // Here you could add additional error handling logic
                // Like sending notifications or storing in a database
            }
        },
    });
}

runConsumer().catch(console.error);