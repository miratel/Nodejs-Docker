const redis = require('redis');
const logger = require('./logger');

const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
    socket: {
        reconnectStrategy: (retries) => {
            if (retries > 20) {
                logger.error('❌ Too many Redis retries, giving up.');
                return new Error('Max retries reached');
            }
            return Math.min(retries * 100, 5000);
        }
    }
});

redisClient.on('error', (err) => logger.error('Redis Client Error:', err));
redisClient.on('connect', () => logger.info('✅ Redis client connected successfully.'));
redisClient.on('reconnecting', () => logger.warn('Redis client is reconnecting...'));

// Connect immediately
(async () => {
    try {
        await redisClient.connect();
    } catch (err) {
        logger.error('Failed to connect to Redis on initial setup:', err);
    }
})();

module.exports = redisClient;