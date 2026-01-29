const callService = require('../../callService');
const logger = require('../../../utils/logger');

function register(manager, io) {
    logger.info('Registering AMI Call Event Handlers...');

    manager.on('Newchannel', async (evt) => {
        try {
            await callService.newCallChannel(evt);
            const activeCalls = await callService.getActiveCalls();
            io.emit('activeCall', { activeCalls });
        } catch (err) {
            logger.error('Error handling Newchannel event:', err);
        }
    });

    manager.on('Newstate', async (evt) => {
        try {
            await callService.newCallState(evt);
            const activeCalls = await callService.getActiveCalls();
            io.emit('activeCall', { activeCalls });
        } catch (err) {
            logger.error('Error handling Newstate event:', err);
        }
    });

    manager.on('Hangup', async (evt) => {
        try {
            await callService.callHangup(evt);
            const activeCalls = await callService.getActiveCalls();
            io.emit('activeCall', { activeCalls });
            io.emit('updateStatistic', {}); // Notify UI to refresh stats
        } catch (err) {
            logger.error('Error handling Hangup event:', err);
        }
    });
    
    manager.on('BlindTransfer', async (evt) => {
        try {
            await callService.handleBlindTransfer(evt);
            const activeCalls = await callService.getActiveCalls();
            io.emit('activeCall', { activeCalls });
        } catch (err) {
            logger.error('Error handling BlindTransfer event:', err);
        }
    });
    
    manager.on('ChanSpyStart', async (evt) => {
        try {
            await callService.updateChanSpyStart(evt);
            const activeCalls = await callService.getActiveCalls();
            io.emit('activeCall', { activeCalls });
        } catch (err) {
            logger.error('Error handling ChanSpyStart event:', err);
        }
    });

    manager.on('ChanSpyStop', async (evt) => {
        try {
            await callService.updateChanSpyStop(evt);
            const activeCalls = await callService.getActiveCalls();
            io.emit('activeCall', { activeCalls });
        } catch (err) {
            logger.error('Error handling ChanSpyStop event:', err);
        }
    });

    // Add other call-related events here (Hold, Unhold, etc.)
}

module.exports = { register };