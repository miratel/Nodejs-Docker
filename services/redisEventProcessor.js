const logger = require('../utils/logger');
const odooService = require('./odoosevice');
const redis = require('redis');
const { v4: uuidv4 } = require('uuid');

class RedisEventProcessor {
    constructor() {
        this.redisClient = redis.createClient({
            url: process.env.REDIS_URL || 'redis://localhost:6379',
            socket: {
                reconnectStrategy: (retries) => {
                    if (retries > 20) {
                        return new Error('Max retries reached');
                    }
                    return Math.min(retries * 100, 5000);
                }
            }
        });

        this.processing = false;
        this.shutdownRequested = false;
        this.status = 'disconnected';
        this.lastError = null;
        this.processedCount = 0;
        this.failedCount = 0;
        this.lastProcessedEvent = null;

        // Bind methods to maintain 'this' context
        this.isConnected = this.isConnected.bind(this);
        this.getStatus = this.getStatus.bind(this);
        this.getMetrics = this.getMetrics.bind(this);
    }

    isConnected() {
        return this.redisClient?.isOpen;
    }

    getStatus() {
        return {
            status: this.status,
            processedCount: this.processedCount,
            failedCount: this.failedCount,
            lastError: this.lastError,
            lastProcessedEvent: this.lastProcessedEvent
        };
    }

    async getMetrics() {
        try {
            return {
                connected: this.isConnected(),
                queueSize: await this.getQueueSize(),
                processedCount: this.processedCount,
                failedCount: this.failedCount,
                status: this.status
            };
        } catch (err) {
            logger.error('Failed to get metrics:', err);
            return {
                error: err.message
            };
        }
    }

    async getQueueSize() {
        if (!this.isConnected()) return -1;
        try {
            return await this.redisClient.lLen('ami:events');
        } catch (err) {
            logger.error('Failed to get queue size:', err);
            return -1;
        }
    }

    async initialize() {
        this.status = 'initializing';

        try {
            await this.connectRedis();
            this.status = 'connected';
            this.processEvents();
        } catch (err) {
            this.status = 'error';
            this.lastError = err.message;
            logger.error('❌ RedisEventProcessor initialization failed:', err);
            throw err;
        }
    }

    async connectRedis() {
        let retries = 0;
        const maxRetries = 5;

        this.redisClient.on('error', (err) => {
            logger.error('Redis error:', err);
            this.status = 'error';
            this.lastError = err.message;
        });

        this.redisClient.on('connect', () => {
            logger.info('✅ Redis connected');
            this.status = 'connected';
            this.lastError = null;
        });

        this.redisClient.on('reconnecting', () => {
            logger.info('Redis reconnecting...');
            this.status = 'reconnecting';
        });

        while (retries < maxRetries && !this.shutdownRequested) {
            try {
                await this.redisClient.connect();
                return;
            } catch (err) {
                retries++;
                this.lastError = err.message;
                logger.warn(`Redis connection attempt ${retries} failed (${err.message})`);

                if (retries < maxRetries) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
        }

        throw new Error(`❌ Failed to connect to Redis after ${maxRetries} attempts`);
    }

    async processEvents() {
        if (this.processing || this.shutdownRequested) return;

        this.processing = true;
        this.status = 'processing';

        try {
            while (!this.shutdownRequested) {
                try {
                    if (!this.redisClient.isOpen) {
                        logger.warn('Redis connection lost, reconnecting...');
                        await this.connectRedis();
                        continue;
                    }

                    const result = await this.redisClient.blPop('ami:events', 10);

                    if (!result || !result.element) {
                        continue;
                    }

                    const event = JSON.parse(result.element);
                    this.lastProcessedEvent = {
                        type: event.type,
                        uniqueid: event.uniqueid,
                        timestamp: new Date(event.timestamp).toISOString()
                    };

                    try {
                        await this.handleEvent(event);
                        this.processedCount++;
                    } catch (err) {
                        this.failedCount++;
                        logger.error('❌ Error processing event:', err);
                        await this.redisClient.rPush('ami:failed_events', JSON.stringify(event));
                    }
                } catch (err) {
                    this.lastError = err.message;
                    logger.error('❌ Event processing error:', err);
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        } finally {
            this.processing = false;
            this.status = 'idle';

            if (!this.shutdownRequested) {
                setTimeout(() => this.processEvents(), 1000);
            }
        }
    }

    async handleEvent(event) {
        const { type, data } = event;
        const eventId = uuidv4();

        logger.info(`Processing [${type}] event for Odoo...`);

        try {
            const handlers = {
                'Newchannel': () => odooService.call('asterisk_plus.channel', 'on_ami_new_channel', [data]),
                'Newstate': () => odooService.call('asterisk_plus.channel', 'on_ami_update_channel_state', [data]),
                'Hangup': () => odooService.call('asterisk_plus.channel', 'on_ami_hangup', [data]),
                'VarSet': () => {
                    if (data.Variable === 'MIXMONITOR_FILENAME') {
                        return odooService.call('asterisk_plus.channel', 'update_recording_filename', [data]);
                    }
                },
                'PeerStatus': () => odooService.call('asterisk_plus.user', 'update_status', [data]),
                'ExtensionStatus': () => odooService.call('asterisk_plus.user', 'on_extension_status', [data]),
                'OriginateResponse': () => odooService.call('asterisk_plus.channel', 'on_ami_originate_response_failure', [data]),
                'response': () => {
                    let res = '';
                    let message = '';
                    if (data.Ping) {
                        message = `<div><p>Ping VoIP Server: ${data.Response}<p></div>
                        <div><p>Time stamp: ${new Date().toISOString()}<p></div> `
                        res = { 'message': message, 'title': 'OdooPBX', 'notify_uid': data.ActionID, 'sticky': false, 'warning': false }
                    }
                    if (data.CoreStartupDate) {
                        message = `<div><p>Core Startup Date: ${data.CoreStartupDate}<p></div>
                        <div><p>Core Startup Time: ${data.CoreStartupTime}<p></div>
                        <div><p>Core Reload Date: ${data.CoreReloadDate}<p></div>
                        <div><p>Core Reload Time: ${data.CoreReloadTime}<p></div>
                        <div><p>Core Current Calls: ${data.CoreCurrentCalls}<p></div>`
                    }
                    res = {
                        'message': message, 'title': 'OdooPBX', 'notify_uid': data.ActionID,
                        'sticky': true, 'warning': false
                    }



                    odooService.call('asterisk_plus.settings', 'odoo_pbx_notify_1', [res]);

                },
                'ParkedCall': () => odooService.call('asterisk_plus.channel', 'on_ami_call_park', [data]),
                'ParkedCallGiveUp': () => odooService.call('asterisk_plus.channel', 'on_ami_call_unpark', [data]),
            };
            const handler = handlers[type];
            if (handler) {
                await handler();
                // logger.debug(`[${eventId}] Successfully send event ${type} to Odoo`);
            }
        } catch (err) {
            logger.error(`[${eventId}] Failed to send event ${type} to Odoo:`, err);
            throw err;
        }
    }

    async shutdown() {
        this.shutdownRequested = true;
        this.status = 'shutting down';

        try {
            if (this.redisClient) {
                await this.redisClient.quit();
            }
            this.status = 'shutdown';
        } catch (err) {
            logger.error('❌ RedisEventProcessor shutdown error:', err);
            this.status = 'error';
        }
    }
}

module.exports = new RedisEventProcessor();