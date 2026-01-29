const queueService = require('../../queueService');
const logger = require('../../../utils/logger');

async function broadcastQueueState(io) {
    const [queues, members] = await Promise.all([
        queueService.getAllQueues(),
        queueService.getAllMembers()
    ]);
    io.emit('queueParams', { queues });
    io.emit('queueMember', { members });
}

function register(manager, io) {
    logger.info('Registering AMI Queue Event Handlers...');

    const queueEvents = [
        'QueueParams', 'QueueMember', 'QueueMemberStatus', 'QueueMemberAdded',
        'QueueMemberRemoved', 'QueueMemberPause', 'QueueCallerJoin', 'QueueCallerLeave',
        'QueueCallerAbandon', 'AgentCalled', 'AgentConnect', 'AgentComplete'
    ];

    queueEvents.forEach(event => {
        manager.on(event, async (evt) => {
            try {
                // Find the correct service method based on event name
                const handler = queueService[`handle${event}`];
                if (typeof handler === 'function') {
                    await handler.call(queueService, evt);
                } else {
                    logger.warn(`No handler found for queue event: ${event}`);
                }
                // After any queue event, broadcast the latest state
                await broadcastQueueState(io);
            } catch (err) {
                logger.error(`Error handling ${event} event:`, err);
            }
        });
    });

    manager.on('QueueStatusComplete', async () => {
         await broadcastQueueState(io);
    });
}

module.exports = { register };