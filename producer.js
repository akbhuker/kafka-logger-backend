const { Kafka } = require('kafkajs');
const express = require('express');
const app = express();

const kafka = new Kafka({
    clientId: 'log-producer',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

app.use(express.json());

// Initialize producer
async function initProducer() {
    await producer.connect();
    console.log('Producer connected');
}

// API endpoint to receive logs
app.post('/logs', async (req, res) => {
    try {
        const { service, level, message } = req.body;

        await producer.send({
            topic: 'logs',
            messages: [
                {
                    key: service,
                    value: JSON.stringify({
                        service,
                        level,
                        message,
                        timestamp: new Date().toISOString()
                    })
                }
            ]
        });

        res.status(200).json({ status: 'Log sent successfully' });
    } catch (error) {
        console.error('Error sending log:', error);
        res.status(500).json({ error: 'Failed to send log' });
    }
});

app.listen(8000, () => {
    console.log('Producer service listening on port 8000');
    initProducer();
});