const logger = require('../../../utils/logger');
const redisEventProcessor = require('../../redisEventProcessor');

// For events that need to be processed asynchronously by Odoo
function queueForProcessing(evt) {
    if (!evt.Event) return;
    const eventData = {
        type: evt.Event,
        data: evt,
        timestamp: Date.now()
    };
    redisEventProcessor.queueEvent(eventData);
}

function register(manager, io) {
    logger.info('Registering other AMI Event Handlers...');

    // These events are important for Odoo, so we queue them via Redis
    const eventsToQueue = [
        'VarSet', // For recording filenames
        'Newstate',
        'Hangup',
        'Newchannel',
        'PeerStatus',
        'ExtensionStatus',
        'OriginateResponse',
        'ParkedCall',
        'ParkedCallGiveUp',
        'response' // For Ping, CoreStatus, etc.
    ];

    eventsToQueue.forEach(event => {
        manager.on(event, (evt) => {
            // Some events might be handled both in real-time and queued
            // Here we just focus on queuing them for backend processing.
            queueForProcessing(evt);
        });
    });
}

module.exports = { register };