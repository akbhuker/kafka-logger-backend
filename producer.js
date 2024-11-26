require('dotenv').config();
const { kafka } = require("./client");
const express = require('express');
const app = express();
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');

// Enhanced Logging Middleware
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
    next();
});

app.use(express.json());
app.use(cors({
    origin: ['http://localhost:3000', process.env.URL],
    methods: ['GET', 'POST']
}));

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: ['http://localhost:3000', process.env.URL],
        methods: ["GET", "POST"]
    }
});

const producer = kafka.producer();
const consumer = kafka.consumer({
    groupId: 'websocket-log-group',
    allowAutoTopicCreation: true
});

// Centralized Error Handling
function handleError(context, error) {
    console.error(`[${context}] Error:`, error);
    // Optional: Send error to logging service or monitoring system
}

// Enhanced Kafka Consumer with Retry and Error Handling
async function runKafkaConsumer() {
    try {
        await consumer.connect();
        console.log('Kafka Consumer Connected');

        await consumer.subscribe({
            topic: 'logs',
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    const logData = JSON.parse(message.value.toString());

                    // Debug logging
                    console.log('Received log from Kafka:', logData);

                    // Ensure io is ready before emitting
                    if (io) {
                        // Broadcast log to all connected clients
                        io.emit('newLog', {
                            ...logData,
                            timestamp: new Date().toISOString()
                        });
                        console.log('Emitted log to WebSocket clients');
                    } else {
                        console.warn('Socket.IO not initialized');
                    }
                } catch (parseError) {
                    handleError('Message Processing', parseError);
                }
            },
            // Add retry configuration
            retry: {
                initial: 1000,
                multiplier: 1.5,
                retries: 3
            }
        });
    } catch (error) {
        handleError('Kafka Consumer Setup', error);
    }
}

// WebSocket Connection Handling with Enhanced Logging
io.on('connection', (socket) => {
    console.log(`[${new Date().toISOString()}] New client connected: ${socket.id}`);

    // Optional: Send initial connection confirmation
    socket.emit('connection', {
        message: 'Successfully connected to log streaming',
        timestamp: new Date().toISOString()
    });

    socket.on('disconnect', (reason) => {
        console.log(`[${new Date().toISOString()}] Client disconnected: ${socket.id}, Reason: ${reason}`);
    });
});

// Initialize Kafka Producer
async function initProducer() {
    try {
        console.log("Connecting Producer");
        await producer.connect();
        console.log("Producer Connected Successfully");
    } catch (error) {
        handleError('Producer Initialization', error);
    }
}

// Comprehensive Log Endpoint with Error Handling
app.post('/logs', async (req, res) => {
    try {
        const { service, level, message } = req.body;

        // Validate input
        if (!service || !level || !message) {
            return res.status(400).json({
                error: 'Missing required fields',
                requiredFields: ['service', 'level', 'message']
            });
        }

        const logMessage = {
            service,
            level,
            message,
            timestamp: new Date().toISOString()
        };

        await producer.send({
            topic: 'logs',
            messages: [{
                key: service,
                value: JSON.stringify(logMessage)
            }]
        });

        // Optional: Also emit directly if needed
        if (io) {
            io.emit('newLog', logMessage);
        }

        res.status(200).json({
            status: 'Log sent successfully',
            log: logMessage
        });
    } catch (error) {
        handleError('Log Endpoint', error);
        res.status(500).json({
            error: 'Failed to send log',
            details: error.message
        });
    }
});

// Graceful Shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down gracefully');
    await producer.disconnect();
    await consumer.disconnect();
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});

const PORT = process.env.PORT || 4000;

server.listen(PORT, async () => {
    console.log(`Server running on port ${PORT}`);
    await initProducer();
    await runKafkaConsumer();
});