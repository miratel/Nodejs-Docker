const AmiClient = require('asterisk-ami-client');
const logger = require('../../utils/logger');
const { registerEventHandlers } = require('./eventHandlers');

class AmiApiService {
    constructor() {
        this.manager = new AmiClient({
            reconnect: true,
            keepAlive: true,
            emitEventsByTypes: true,
            emitResponsesById: true, // Important for tracking action responses
        });
        this.io = null;
        this.amiConnected = false;
        this.status = 'disconnected';

        this.manager.on('connect', () => {
            logger.info('✅ AMI connected successfully.');
            this.amiConnected = true;
            this.status = 'connected';
            this.postConnectActions();
        });

        this.manager.on('error', (error) => {
            logger.error('❌ AMI connection error:', error);
            this.amiConnected = false;
            this.status = 'error';
        });

        this.manager.on('close', () => {
            logger.warn('AMI connection closed. Will attempt to reconnect.');
            this.amiConnected = false;
            this.status = 'disconnected';
        });
    }

    setIo(ioInstance) {
        this.io = ioInstance;
    }

    async initialize(config, ioInstance) {
        this.status = 'initializing';
        this.setIo(ioInstance);

        return new Promise((resolve, reject) => {
            this.manager.once('connect', resolve); // Resolve promise on first successful connect
            this.manager.once('error', reject);   // Reject if first connection attempt fails

            this.manager.connect(
                config.ami_user,
                config.ami_password,
                { host: config.ami_host, port: config.ami_port }
            );

            // Register all event handlers from separate files
            registerEventHandlers(this.manager, this.io);
        });
    }

    postConnectActions() {
        logger.info('Performing post-connect AMI actions (fetching initial state)...');
        this.manager.action({ Action: 'Sippeers' });
        this.manager.action({ Action: 'QueueStatus' });
        this.manager.action({ Action: 'ParkedCalls' });
        this.manager.action({ Action: 'ExtensionStateList' });
    }

    // --- Action Methods ---
    // These methods now just send actions to Asterisk.
    // The logic is handled by event handlers.

    asteriskPing = (id) => this.manager.action({ Action: 'Ping', ActionID: id });
    reloadAction = (id) => this.manager.action({ Action: 'Reload', ActionID: id });
    coreStatusAction = (id) => this.manager.action({ Action: 'CoreStatus', ActionID: id });
    hangupAction = (channel) => this.manager.action({ Action: 'Hangup', Channel: channel });
    originateCall = (params) => this.manager.action({ Action: 'Originate', ...params });
    spyCall = (params) => this.manager.action({ Action: 'Originate', Application: 'ChanSpy', ...params });

    // ... (Add other action methods here in this simple format)

    async shutdown() {
        if (this.manager && this.amiConnected) {
            logger.info('Disconnecting from AMI...');
            this.manager.disconnect();
            this.status = 'shutdown';
        }
    }
}

module.exports = new AmiApiService();