const peerService = require('../../peerService');
const logger = require('../../../utils/logger');

function register(manager, io) {
    logger.info('Registering AMI Peer Event Handlers...');

    manager.on('PeerEntry', (evt) => peerService.peerEntry(evt).catch(err => logger.error(err)));

    manager.on('PeerlistComplete', async () => {
        try {
            await peerService.syncPeerNamesFromDb(manager);
            const peers = await peerService.getAllPeers();
            io.emit('PeerStatus', { peers });
        } catch (err) {
            logger.error('Error handling PeerlistComplete:', err);
        }
    });

    manager.on('PeerStatus', async (evt) => {
        try {
            await peerService.updatePeerStatus(evt);
            const peers = await peerService.getAllPeers();
            io.emit('PeerStatus', { peers });
        } catch (err) {
            logger.error('Error handling PeerStatus event:', err);
        }
    });

    manager.on('ExtensionStatus', async (evt) => {
        try {
            await peerService.updateExtensionStatus(evt);
            const peers = await peerService.getAllPeers();
            io.emit('PeerStatus', { peers });
        } catch (err) {
            logger.error('Error handling ExtensionStatus event:', err);
        }
    });
    
    manager.on('DBGetResponse', async (evt) => {
        // This is used for fetching peer names
        try {
            if (evt.Family === 'AMPUSER' && evt.Key.includes('/cidname')) {
                await peerService.updatePeerNameFromDb(evt);
            }
        } catch(err) {
            logger.error('Error handling DBGetResponse event:', err);
        }
    });
}

module.exports = { register };