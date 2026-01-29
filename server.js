const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const logger = require('./utils/logger');
// const AMIService = require('./services/amiService');
const AmiApiService = require('./services/ami_api');
const RedisEventProcessor = require('./services/redisEventProcessor');
const odooService = require('./services/odoosevice');
const path = require('path');
const socketio = require('socket.io');
const { handleAgentAction } = require('./services/actionHandlers');
// Load environment variables first
dotenv.config();

const { exec } = require('child_process');
// const MICROSIP_PATH = process.env.MICROSIP_PATH || 'C:\\Users\\alial\\AppData\\Local\\MicroSIP\\microsip.exe';
// Validate requnired environment variables
const requiredEnvVars = [
    'AMI_HOST', 'AMI_PORT', 'AMI_USER', 'AMI_PASSWD',
    'ODOO_URL', 'ODOO_DATABASE', 'ODOO_USERNAME', 'ODOO_PASSWORD',
    'REDIS_URL'
];

for (const envVar of requiredEnvVars) {
    if (!process.env[envVar]) {
        logger.error(`Missing required environment variable: ${envVar}`);
        process.exit(1);
    }
}

const app = express();

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Initialize services
let server;
let serviceInitialized = false;

async function initializeServices() {
    try {
        logger.info('🔌 Initializing services...');

        // Initialize Redis Event Processor first
        await RedisEventProcessor.initialize();

        // Initialize Odoo Service
        await odooService.initialize();

        let server = await odooService.getPBXconfiguration()
        // Initialize AMI Service
        if (odooService.authenticated) await AmiApiService.initialize(server.ami_host, server.ami_port, server.ami_user, server.ami_password, io);

        serviceInitialized = true;
        logger.info('✅ All services initialized successfully');
    } catch (err) {
        logger.error('❌ Service initialization failed:', err);
        throw err;
    }
}

// Start HTTP server
try {
    const PORT = parseInt(process.env.PORT) || 3001;
    server = app.listen(PORT, async () => {
        logger.info(`🚀 Backend server is live on port: ${PORT}`);
        try {
            await initializeServices();
        } catch (err) {
            logger.error('❌ Failed to initialize services:', err);
            // Don't exit immediately - server can run in degraded mode
        }
    });

    server.on('error', (err) => {
        logger.error('Server error:', err);
        process.exit(1);
    });
} catch (err) {
    logger.error('❌ Failed to start server:', err);
    process.exit(1);
}
const io = socketio(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Routes
app.post('/app/odoo_pbx/agent', async (req, res) => {
    if (!serviceInitialized) {
        return res.status(503).json({ error: 'Service initializing, try again later' });
    }

    try {
        console.log(req.body)
        // if (req.body.fun === 'test.ping') return res.status(400).json({ Response: 'Success', Ping: 'Pong' });
        // const params = req.body?.args;
        // if (!params) {
        //     return res.status(400).json({ error: 'Missing args in request body' });
        // }
        // logger.info('Agent action requested:', params);
        // // console.log(req.body)
        // switch (params.Action) {
        //     case 'Ping':
        //         AmiApiService.asteriskPing(req.body.res_notify_uid);
        //         break;
        //     case 'Reload':
        //         AmiApiService.reloadAction(req.body.res_notify_uid);
        //         break;
        //     case 'CoreStatus':
        //         AmiApiService.coreStatusAction(req.body.res_notify_uid);
        //         break;
        //     case 'Hangup':
        //         AmiApiService.hangupAction(params.Channel);
        //         break;
        //     case 'Park':
        //         AmiApiService.parkAction(params);
        //         break;
        //     case 'Redirect':
        //         AmiApiService.unParkCall(params);
        //         break;
        //     case 'BlindTransfer':
        //         AmiApiService.BlindTransferAction(params);
        //         break;
        //     case 'Originate':
        //         AmiApiService.originateCall(params);
        //         break;
        //     case 'OriginateSpy':
        //         AmiApiService.spyCall(params);
        //         break;
        //     case 'SetCallForward':
        //         AmiApiService.SetCallForwardAction(params);
        //         break;
        //     case 'CancelCallForward':
        //         AmiApiService.CancelCallForwardAction(params);
        //         break;
        //     case 'SetCwDnd':
        //         AmiApiService.SetCwDndAction(params);
        //         break;
        //     case 'UnSetCwDnd':
        //         AmiApiService.UnSetCwDndAction(params);
        //         break;
        //     case 'setUserName':
        //         AmiApiService.setUserName(params);
        //         break;

        // }

        // io.emit('sipUpdate', params.Action);
        // res.status(200).json({ status: 'success' });
        const result = await handleAgentAction(req.body);
        console.log(result);
        res.status(200).json(result);

    } catch (err) {
        logger.error('Agent action processing error:', err);
        res.status(err.statusCode || 500).json({ error: err.message || 'Internal Server Error' });
    }
});
app.use(express.static('web'));

app.get('/Phone', async (req, res) => {
    const _retfile = path.join(__dirname, 'web', 'index.html');
    res.sendFile(path.resolve(__dirname, 'web', 'index.html'))
})

app.get('/health', async (req, res) => {
    AmiApiService.asteriskPing()
    // odooService.getPBXconfiguration()
    try {
        const health = {
            status: serviceInitialized ? 'ok' : 'initializing',
            services: {
                odoo: {
                    connected: odooService.authenticated,
                    status: odooService.getStatus()
                },
                ami: {
                    connected: AmiApiService.isConnected(),
                    status: AmiApiService.getStatus()
                },
                redis: {
                    connected: RedisEventProcessor.isConnected(),
                    status: RedisEventProcessor.getStatus()
                }
            },
            uptime: process.uptime(),
            timestamp: new Date().toISOString()
        };

        const statusCode = serviceInitialized ? 200 : 503;
        res.status(statusCode).json(health);
    } catch (err) {
        res.status(500).json({
            status: 'error',
            error: err.message
        });
    }
});

// Update the metrics endpoint
app.get('/metrics', async (req, res) => {
    try {
        const metrics = {
            status: 'ok',
            system: {
                uptime: process.uptime(),
                memory: process.memoryUsage(),
                cpu: process.cpuUsage()
            },
            services: {
                odoo: await odooService.getMetrics(),
                ami: await AmiApiService.getMetrics(),
                redis: await RedisEventProcessor.getMetrics()
            },
            timestamp: new Date().toISOString()
        };

        res.status(200).json(metrics);
    } catch (err) {
        res.status(500).json({
            status: 'error',
            error: err.message
        });
    }
});
// Error handling middleware
app.use((err, req, res, next) => {
    logger.error('Request error:', err);
    res.status(500).json({ error: 'Internal server error' });
});

// Process termination handlers
process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);
async function gracefulShutdown() {
    logger.info('SIGINT/SIGTERM received. Shutting down gracefully...');
    try {
        await AmiApiService.shutdown();
        await RedisEventProcessor.shutdown();
        if (redisClient.isOpen) {
            await redisClient.quit();
        }
        logger.info('All services closed. Goodbye! 👋');
        process.exit(0);
    } catch (err) {
        logger.error('Error during graceful shutdown:', err);
        process.exit(1);
    }
}

// async function gracefulShutdown() {
//     logger.info('Received shutdown signal, terminating gracefully...');

//     try {
//         // Close services in order
//         await AmiApiService.shutdown();
//         await RedisEventProcessor.shutdown();
//         await odooService.shutdown();

//         // Close server
//         if (server) {
//             server.close(() => {
//                 logger.info('HTTP server closed');
//                 process.exit(0);
//             });
//         }

//         // Force exit after timeout
//         setTimeout(() => {
//             logger.warn('Forcing shutdown after timeout');
//             process.exit(1);
//         }, 10000);
//     } catch (err) {
//         logger.error('Graceful shutdown failed:', err);
//         process.exit(1);
//     }
// }

app.post('/soft_phone/command', async (req, res) => {
    const { command, path } = req.body;
    console.log(req.body)

    // Validate command
    const allowedCommands = ['answer', 'hangup', 'hangupall', 'transfer'];
    // if (!allowedCommands.includes(command.split(':')[0])) {
    //     return res.status(400).json({ error: 'Invalid command' });
    // }

    try {
        exec(`"${path}" /${command}`, (error, stdout, stderr) => {
            if (error) {
                logger.error('SoftPhone command failed:', error);
                return res.status(500).json({ error: 'Command execution failed' });
            }
            res.status(200).json({ status: 'success' });
        });
    } catch (err) {
        logger.error('SoftPhone route error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Unhandled rejection/exception handlers
process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception:', error);
    // Don't exit immediately for uncaught exceptions - try to keep running
});
