const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'analytics-consumer',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'analytics-group' });

// Simple in-memory analytics
const serviceMetrics = new Map();

async function runConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topic: 'logs', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const logData = JSON.parse(message.value.toString());

            // Update metrics
            if (!serviceMetrics.has(logData.service)) {
                serviceMetrics.set(logData.service, {
                    total: 0,
                    byLevel: {}
                });
            }

            const metrics = serviceMetrics.get(logData.service);
            metrics.total += 1;
            metrics.byLevel[logData.level] = (metrics.byLevel[logData.level] || 0) + 1;

            // Print current metrics every 10 messages
            if (metrics.total % 10 === 0) {
                console.log('\nCurrent Metrics for', logData.service);
                console.log('Total logs:', metrics.total);
                console.log('By level:', metrics.byLevel);
                console.log('-------------------');
            }
        },
    });
}

runConsumer().catch(console.error);