const { kafka } = require("./client");
const express = require('express');
const app = express();
const cors = require('cors');

app.use(express.json());
app.use(cors());

const producer = kafka.producer();

// Initialize producer
async function initProducer() {
    console.log("Connecting Producer");

    await producer.connect();
    console.log("Producer Connected Successfully");
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