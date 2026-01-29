const logger = require('../utils/logger');
const odooService = require('./odoosevice');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const AmiClient = require('asterisk-ami-client');
const redis = require('redis');
const PeerService = require('./peerService');
const CallService = require('./callService');
const QueueService = require('./queueService');
// const { Channel } = require('diagnostics_channel');
const { exec } = require('child_process');
const callService = require('./callService');
// const sipPeers = {};
class AmiApiService {
    constructor() {
        this.manager = null;
        this.io = null;
        this.sippeers = null;
        this.queueParams = null;
        this.member = null;
        this.activeCall = null;
        this.park = null;
        this.redisClient = redis.createClient({
            url: process.env.REDIS_URL || 'redis://localhost:6379',
            socket: {
                reconnectStrategy: (retries) => {
                    if (retries > 10) {
                        logger.error('Too many Redis retries, giving up');
                        return new Error('Max retries reached');
                    }
                    return Math.min(retries * 100, 5000);
                }
            }
        });

        this.redisConnected = false;
        this.amiConnected = false;
        this.status = 'disconnected';

        this.redisClient.on('error', (err) => {
            logger.error('❌ Redis client error:', err);
            this.redisConnected = false;
        });

        this.redisClient.on('connect', () => {
            logger.info('✅ Redis client connected');
            this.redisConnected = true;
        });

        this.redisClient.on('reconnecting', () => {
            logger.info('Redis client reconnecting');
            this.redisConnected = false;
        });
    }

    isConnected() {
        return this.amiConnected && this.redisConnected;
    }

    getStatus() {
        return {
            ami: this.amiConnected ? 'connected' : 'disconnected',
            redis: this.redisConnected ? 'connected' : 'disconnected',
            status: this.status
        };
    }

    async getMetrics() {
        return {
            connected: this.amiConnected,
            lastEventTime: this.lastEventTime || null,
            eventQueueSize: await this.getQueueSize()
        };
    }

    async getQueueSize() {
        if (!this.redisConnected) return 0;
        try {
            return await this.redisClient.lLen('ami:events');
        } catch (err) {
            logger.error('❌ Failed to get queue size:', err);
            return -1;
        }
    }

    async initialize(ami_host, ami_port, ami_user, ami_password, io) {
        this.status = 'initializing';

        try {
            await this.connectRedis();
            await this.connectAMI(ami_host, ami_port, ami_user, ami_password);
            this.status = 'connected';
            this.io = io;
            this.io.on('connection', (socket) => {
                socket.on('disconnect', () => {
                    console.log('Client disconnected ', socket.id);

                });
                console.log('Client connected ', socket.id);
                socket.emit('hello', socket.id)
                // socket.on('event_name', (evt => {
                // this.manager.action({
                //     Action: 'Sippeers'
                // });
                // this.manager.action({
                //     Action: 'QueueStatus'
                // });
                // console.log(evt)
                // }))
                socket.on('spy_call', (evt => {
                    console.log(evt)
                }))
                socket.on('getInitData', (evt => {

                    this.io.emit('PeerStatus', { peers: this.sippeers });
                    this.io.emit('queueParams', { queue: this.queueParams });
                    this.io.emit('queueMember', { member: this.member });
                    this.io.emit('ParkStatus', { park: this.park });
                    if (this.activeCall)
                        this.io.emit('activeCall', { activeCall: this.activeCall });
                    console.log(evt)

                }))
                socket.on('auto_answer', (evt => {
                    try {
                        if (evt.timeout != 'now') {
                            setTimeout(() => {
                                exec(`"${evt.path}" /${evt.command}`, (error, stdout, stderr) => {
                                    if (error) {
                                        logger.error('SoftPhone command failed:', error);
                                    }
                                    logger.info('Auto Answer Process');
                                    socket.emit('isAutoAnswer', { status: 'success' })
                                });
                            }, evt.timeout)
                        }
                        else {
                            exec(`"${evt.path}" /${evt.command}`, (error, stdout, stderr) => {
                                if (error) {
                                    logger.error('SoftPhone command failed:', error);
                                }
                                logger.info('Auto Answer Process');
                                socket.emit('isAutoAnswer', { status: 'success' })
                            });
                        }

                    } catch (err) {
                        logger.error('SoftPhone route error:', err);
                    }
                    console.log(evt)
                }))

            });
            // this.io.off('event_name')
            // this.reloadAction();
            this.manager.action({
                Action: 'Sippeers'
            });
            this.manager.action({
                Action: 'QueueStatus'
            });
            this.manager.action({
                Action: 'ParkedCalls',
                ParkingLot: '',
            });
            this.manager.action({
                Action: 'Parkinglots',
            });
            this.manager.action({
                Action: 'ExtensionStateList'
            });
            // this.manager.action({
            //     Action: 'DBGet',
            //     Family: 'AMPUSER',
            //     Key: '200/cidname',
            //     // Val: params.Val,
            // });



        } catch (err) {
            this.status = 'error';
            logger.error('❌ AMI service initialization failed:', err);
            throw err;
        }

    }

    async connectRedis() {
        let retries = 0;
        const maxRetries = 5;

        while (retries < maxRetries) {
            try {
                await this.redisClient.connect();
                this.redisConnected = true;
                return;
            } catch (err) {
                retries++;
                logger.warn(`Redis connection attempt ${retries} failed (${err.message})`);

                if (retries < maxRetries) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
        }

        throw new Error(`❌ Failed to connect to Redis after ${maxRetries} attempts`);
    }

    async connectAMI(ami_host, ami_port, ami_user, ami_password) {
        return new Promise((resolve, reject) => {
            this.manager = new AmiClient({
                reconnect: true,
                keepAlive: true,
                emitEventsByTypes: true,
                emitResponsesById: true
            });

            this.manager.on('connect', () => {
                logger.info('✅ AMI connected successfully.');
                this.amiConnected = true;
                resolve();
            });

            this.manager.on('error', (error) => {
                logger.error('AMI error:', error);
                this.amiConnected = false;
                if (!this.manager.connected) {
                    reject(error);
                }
            });

            this.manager.on('close', () => {
                logger.info('AMI connection closed');
                this.amiConnected = false;
            });
            // this.manager.on('response', (evt) => {
            //     console.log(evt)
            // });



            this.manager.connect(
                ami_user,//process.env.AMI_USER,
                ami_password,//process.env.AMI_PASSWD,
                {
                    host: ami_host,//process.env.AMI_HOST,
                    port: ami_port || 5038,//process.env.AMI_PORT || 5038
                }
            );

            this.setupEventHandlers();
        });
    }

    setupEventHandlers() {
        const eventHandlers = {
            // 'event': this.handleEvent.bind(this),
            'PeerEntry': this.handlePeerEntry.bind(this),
            'PeerlistComplete': this.handlePeerlistComplete.bind(this),
            'Newstate': this.handleNewState.bind(this),
            'Newchannel': this.handleNewChannel.bind(this),
            'Hangup': this.handleHangup.bind(this),
            'VarSet': this.handleMixMonitor.bind(this),
            'PeerStatus': this.handlePeerStatus.bind(this),
            'ExtensionStatus': this.handleExtensionStatus.bind(this),
            'OriginateResponse': this.handleOriginateResponseFailure.bind(this),
            'response': this.handleResponse.bind(this),
            'QueueParams': this.handleQueueParams.bind(this),
            'QueueStatusComplete': this.handleQueueStatusComplete.bind(this),
            'QueueMember': this.handleQueueMember.bind(this),
            'QueueMemberStatus': this.handleQueueMemberStatus.bind(this),
            'QueueMemberAdded': this.handleQueueMemberAdded.bind(this),
            'QueueMemberRemoved': this.handleQueueMemberRemoved.bind(this),
            'QueueMemberPause': this.handleQueueMemberPause.bind(this),
            // 'QueueMemberPenalty': this.handleQueueMemberPenalty.bind(this),
            // 'QueueMemberRinginuse': this.handleQueueMemberRinginuse.bind(this),
            'QueueCallerJoin': this.handleQueueCallerJoin.bind(this),
            'QueueCallerLeave': this.handleQueueCallerLeave.bind(this),
            'QueueCallerAbandon': this.handleQueueCallerAbandon.bind(this),
            'PresenceStateChange': this.handlePresenceStateChange.bind(this),
            // 'PresenceStateListComplete': this.handlePresenceStateListComplete.bind(this),
            'PresenceStatus': this.handlePresenceStatus.bind(this),
            'AgentCalled': this.handleAgentCalled.bind(this),
            'AgentComplete': this.handleAgentComplete.bind(this),
            'AgentConnect': this.handleAgentConnect.bind(this),
            'AgentDump': this.handleAgentDump.bind(this),
            // 'AgentLogin': this.handleAgentLogin.bind(this),
            // 'AgentLogoff': this.handleAgentLogoff.bind(this),
            'AgentRingNoAnswer': this.handleAgentRingNoAnswer.bind(this),
            'BlindTransfer': this.handleBlindTransfer.bind(this),
            'ChanSpyStart': this.handleChanSpyStart.bind(this),
            'ChanSpyStop': this.handleChanSpyStop.bind(this),
            'ChannelTalkingStart': this.handleChannelTalkingStart.bind(this),
            // 'ChannelTalkingStop': this.handleChannelTalkingStop.bind(this),
            // 'ConfbridgeEnd': this.handleConfbridgeEnd.bind(this),
            // 'ConfbridgeJoin': this.handleConfbridgeJoin.bind(this),
            // 'ConfbridgeLeave': this.handleConfbridgeLeave.bind(this),
            // 'ConfbridgeList': this.handleConfbridgeList.bind(this),
            // 'ConfbridgeListRooms': this.handleConfbridgeListRooms.bind(this),
            // 'ConfbridgeMute': this.handleConfbridgeMute.bind(this),
            // 'ConfbridgeRecord': this.handleConfbridgeRecord.bind(this),
            // 'ConfbridgeStart': this.handleConfbridgeStart.bind(this),
            // 'ConfbridgeStopRecord': this.handleConfbridgeStopRecord.bind(this),
            // 'ConfbridgeTalking': this.handleConfbridgeTalking.bind(this),
            // 'ConfbridgeUnmute': this.handleConfbridgeUnmute.bind(this),
            // 'CoreShowChannel': this.handleCoreShowChannel.bind(this),
            // 'DTMFBegin': this.handleDTMFBegin.bind(this),
            // 'DTMFEnd': this.handleDTMFEnd.bind(this),
            // 'DeviceStateChange': this.handleDeviceStateChange.bind(this),
            // 'DeviceStateListComplete': this.handleDeviceStateListComplete.bind(this),
            // 'DialBegin': this.handleDialBegin.bind(this),
            // 'DialEnd': this.handleDialEnd.bind(this),
            // 'DialState': this.handleDialState.bind(this),
            // 'ExtensionStateListComplete': this.handleExtensionStateListComplete.bind(this),
            // 'BridgeListItem': this.handleBridgeListItem.bind(this),
            // 'BridgeListComplete': this.hnadleBridgeListComplete.bind(this),
            // 'QueueSummary': this.handleQueueSummary.bind(this),
            'Hold': this.handleHold.bind(this),
            'Unhold': this.handleUnhold.bind(this),
            // 'MWIGet': this.handleMWIGet.bind(this),
            // 'MWIGetComplete': this.handleMWIGetComplete.bind(this),
            // 'MessageWaiting': this.handleMessageWaiting.bind(thid),
            // 'MiniVoiceMail': this.handleMiniVoiceMail.bind(this),
            // 'NewConnectedLine': this.handleNewConnectedLine.bind(this),
            'Parkinglot': this.handleParkinglot.bind(this),
            'ParkedCall': this.handleParkedCall.bind(this),
            'ParkedCallGiveUp': this.handleParkedCallGiveUp.bind(this),
            'ParkedCallSwap': this.handleParkedCallSwap.bind(this),
            'ParkedCallTimeOut': this.handleParkedCallTimeOut.bind(this),
            'DBGetResponse': this.handleDBGetResponse.bind(this),
            'Cdr': this.handleCdr.bind(this),
            // 'Pickup': this.handlePickup.bind(this),
        };

        Object.entries(eventHandlers).forEach(([event, handler]) => {
            this.manager.on(event, handler);
        });
    }
    async shutdown() {
        try {
            if (this.manager) {
                this.manager.disconnect();
            }
            if (this.redisClient) {
                await this.redisClient.quit();
            }
            this.status = 'shutdown';
        } catch (error) {
            logger.error('AMI service shutdown error:', error);
        }
    }

    async queueEvent(eventType, evt) {
        if (!this.redisConnected) {
            logger.warn('Redis not connected, falling back to direct Odoo send');
            try {
                return await this.sendDirectToOdoo(eventType, evt);
            } catch (error) {
                logger.error('Direct Odoo send failed:', error);
                throw error;
            }
        }

        try {
            const eventData = {
                type: eventType,
                data: evt,
                timestamp: Date.now(),
                uniqueid: evt.Uniqueid || uuidv4()
            };

            await this.redisClient.rPush('ami:events', JSON.stringify(eventData));
            logger.debug(`Event queued in Redis: ${eventType} for ${evt.Uniqueid}`);
        } catch (err) {
            logger.error('Failed to queue event in Redis, falling back to direct send:', err);
            return await this.sendDirectToOdoo(eventType, evt);
        }
    }

    async sendDirectToOdoo(eventType, evt) {
        const model = 'asterisk_plus.channel';
        let method;

        switch (eventType) {
            case 'Newchannel':
                method = 'on_ami_new_channel';
                break;
            case 'Newstate':
                method = 'on_ami_update_channel_state';
                break;
            case 'Hangup':
                method = 'on_ami_hangup';
                break;
            case 'VarSet':
                method = 'update_recording_filename';
                break;
            default:
                throw new Error(`Unknown event type: ${eventType}`);
        }

        return odooService.sendEvent(model, method, evt);
    }

    async handleEvent(evt) {
        console.log('*******handleEvent', evt)
    }

    async handlePeerEntry(evt) {
        // const lowerData = this.lowercaseKeys(evt);
        // console.log(evt)
        PeerService.peerEntry(evt)
        // this.io.emit('PeerStatus', PeerService.getPeers())
    }
    async handlePeerlistComplete(evt) {
        // console.log(PeerService.getPeers())
        // const peers = await PeerService.getPeers();
        // console.log(peers)
        // this.io.emit('PeerStatus', { peers: peers })
        try {
            // await PeerService.setUserNameByExten();
            this.sippeers = await PeerService.getPeers();
            for (let i = 0; i < this.sippeers.length; i++) {
                this.manager.action({
                    Action: 'DBGet',
                    Family: 'AMPUSER',
                    Key: this.sippeers[i].ExtenNumber + '/cidname',
                });
            }

            // console.log(this.sippeers)
            this.io.emit('PeerStatus', { peers: this.sippeers });
        } catch (error) {
            console.error('Error getting peers:', error);
        }
    }

    async handleMixMonitor(evt) {
        if (evt.Variable != 'MIXMONITOR_FILENAME') return;

        try {
            logger.info(`[${evt.CallerIDNum}] Update Recording Filename: ${path.basename(evt.Value)}`);
            await this.queueEvent('VarSet', evt);
        } catch (error) {
            logger.error('Failed to process MIXMONITOR_FILENAME event:', error);
        }
    }

    async handleNewChannel(evt) {
        try {
            // const eventId = uuidv4();
            // console.log(evt)
            // if (evt.Exten === 's') return
            await this.queueEvent('Newchannel', evt);
            CallService.newCallChannel(evt);
            this.activeCall = await CallService.getActiveCall();
            this.io.emit('activeCall', { activeCall: this.activeCall });
            // console.log('newchannel', CallService.getActiveCall())
            logger.info(`New Call From ${evt.CallerIDNum} Start At ${new Date().toISOString()}`);
        } catch (error) {
            logger.error('Failed to process NewChannel event:', error);
        }
    }

    async handleNewState(evt) {
        try {
            // if (evt.CallerIDNum.includes('@')) return
            // console.log(evt)
            // const eventId = uuidv4();
            await this.queueEvent('Newstate', evt);

            CallService.newCallState(evt);
            CallService.handleNewConnectedLine(evt);
            PeerService.updateConnectedLineNum(evt);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            this.activeCall = await CallService.getActiveCall();
            this.io.emit('activeCall', { activeCall: this.activeCall });
            // console.log(peers)
            // console.log('newchannel', CallService.getActiveCall())
            logger.info(`NewState event ${evt.CallerIDNum} processed successfully`);
        } catch (error) {
            logger.error('Failed to process NewState event:', error);
        }
    }

    async handleHangup(evt) {
        try {
            // const eventId = uuidv4();
            // console.log(evt)
            await this.queueEvent('Hangup', evt);
            CallService.callHangup(evt);
            // PeerService.updateConnectedLineNum(evt);
            // PeerService.updateSpyerChannel(evt);
            // PeerService.updateOriginateResponse(evt);
            PeerService.updateOnHangup(evt);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            this.activeCall = await CallService.getActiveCall();
            this.io.emit('activeCall', { activeCall: this.activeCall });
            if (evt.Uniqueid === evt.Linkedid) this.io.emit('updateStatistic', {})
            // console.log('newchannel', CallService.getActiveCall())
            logger.info(`Call ${evt.Uniqueid} ended at ${new Date().toISOString()}`);
        } catch (error) {
            logger.error('Failed to process Hangup event:', error);
        }
    }
    async handlePeerStatus(evt) {
        try {
            // const eventId = uuidv4();
            // console.log(evt)
            await this.queueEvent('PeerStatus', evt);
            PeerService.updatePeerStatus(evt);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            this.member = await QueueService.getQueueMembers();
            this.io.emit('queueMember', { member: this.member });

            // logger.info(`PeerStatus: ${evt.Peer} [${evt.PeerStatus}] => Address: ${evt.Address || ''}`);
        } catch (error) {
            logger.error('Failed to process PeerStatus event:', error);
        }
    }
    async handleExtensionStatus(evt) {
        try {
            // console.log(evt)
            if (evt.Exten.includes('*')) return;
            // console.log(evt)
            await this.queueEvent('ExtensionStatus', evt);
            PeerService.updateExtensionStatus(evt);
            PeerService.updateParkinglotStatus(evt);
            this.sippeers = await PeerService.getPeers();
            this.park = await PeerService.getParkLots();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            this.io.emit('ParkStatus', { park: this.park });
            logger.info(`ExtensionStatus: ${evt.Exten} [${evt.StatusText}]`);
        } catch (error) {
            logger.error('Failed to process ExtensionStatus event:', error);
        }
    }
    async handleOriginateResponseFailure(evt) {
        try {
            // console.log(evt)
            CallService.originateResponse(evt)
            PeerService.updateOriginateResponse(evt);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            if (evt.Response === 'Success') return
            await this.queueEvent('OriginateResponse', evt);
            logger.info(`OriginateResponse: ${evt.Exten} [${evt.Response}]`);
        } catch (error) {
            logger.error('Failed to process OriginateResponse event:', error);
        }
    }
    async handleResponse(evt) {
        try {
            // console.log(evt)
            if (!evt.ActionID) return
            await this.queueEvent('response', evt);
            logger.info(`Ping Response: ${evt.Response} [${evt.Ping}]`);
        } catch (error) {
            logger.error('Failed to process Response event:', error);
        }
    }
    async handleQueueParams(evt) {
        try {
            // console.log(evt)
            QueueService.updateQueueParams(evt);

        } catch (error) {
            logger.error('Failed to process handle Queue Params:', error);
        }

    }
    async handleQueueMember(evt) {
        try {
            QueueService.updateQueueMember(evt)
        } catch (error) {
            logger.error('Failed to process handle Queue Member:', error);
        }

        // console.log(evt)
    }
    async handleQueueMemberStatus(evt) {
        try {
            QueueService.updateQueueMemberStatus(evt);
            this.member = await QueueService.getQueueMembers()
            this.io.emit('queueMember', { member: this.member });
        } catch (error) {
            logger.error('Failed to process handle Queue Member Status:', error);
        }

        // console.log(QueueService.getQueueMembers())
    }
    async handleQueueStatusComplete(evt) {
        try {
            // console.log(evt)
            this.queueParams = await QueueService.getQueueParams();
            this.io.emit('queueParams', { queue: this.queueParams });
            this.member = await QueueService.getQueueMembers()
            this.io.emit('queueMember', { member: this.member });
        } catch (error) {
            logger.error('Failed to process handle Queue Status Complete:', error);
        }

    }
    async handleQueueMemberAdded(evt) {
        try {
            QueueService.addQueueMember(evt);
            this.member = await QueueService.getQueueMembers();
            this.io.emit('queueMember', { member: this.member });
        } catch (error) {
            logger.error('Failed to process handle Queue Member Added:', error);
        }
    }
    async handleQueueMemberRemoved(evt) {
        try {
            QueueService.removedQueueMember(evt);
            this.member = await QueueService.getQueueMembers();
            this.io.emit('queueMember', { member: this.member });
        } catch (error) {
            logger.error('Failed to process handle Queue Member Removed:', error);
        }

    }

    async handleQueueMemberPause(evt) {
        QueueService.updateQueueMemberStatus(evt)
        this.member = await QueueService.getQueueMembers();
        this.io.emit('queueMember', { member: this.member });
    }

    async handleQueueCallerJoin(evt) {
        try {
            // console.log('handleQueueCallerJoin', evt)
            QueueService.addQueueCaller(evt);
            QueueService.updateQueueCallerStatus(evt);
            // const test = await QueueService.getQueueCallers()
            // console.log(test)
            // console.log(test[0]._lastStates)
            this.queueParams = await QueueService.getQueueParams();
            // console.log(caller)

            // this.io.emit('queueCaller', { caller: caller });
            this.io.emit('queueParams', { queue: this.queueParams });

        } catch (error) {
            logger.error('Failed to process handle Queue Caller Join:', error);
        }

        // console.log('queuecallerjoin', QueueService.getQueueCallers())
        // console.log(QueueService.getQueueParams())
    }
    async handleAgentCalled(evt) {
        QueueService.updateQueueCallerStatus(evt);
        QueueService.updateQueueMemberStatus(evt)
        this.member = await QueueService.getQueueMembers();
        // console.log(member)
        this.io.emit('queueMember', { member: this.member });
        // const test = await QueueService.getQueueCallers()
        // console.log(test)
        // console.log(test[0]._lastStates)
        // console.log('handleAgentCalled', evt)
    }
    async handleBlindTransfer(evt) {
        console.log('handleBlindTransfer', evt)
        CallService.handleBlindTransfer(evt);

    }
    async handleAgentRingNoAnswer(evt) {
        QueueService.updateQueueCallerStatus(evt);
        QueueService.updateQueueMemberStatus(evt)
        this.member = await QueueService.getQueueMembers();
        // console.log(member)
        this.io.emit('queueMember', { member: this.member });
        // const test = await QueueService.getQueueCallers()
        // console.log(test)
        // console.log(test[0]._lastStates)
        // console.log('handleAgentRingNoAnswer', evt)
    }
    async handlePresenceStateChange(evt) {
        console.log('handlePresenceStateChange', evt)
    }

    async handlePresenceStatus(evt) {
        console.log('handlePresenceStatus', evt)
    }
    async handleQueueCallerAbandon(evt) {
        // console.log(evt)
        QueueService.updateQueueCallerStatus(evt);
        QueueService.updateQueueMemberStatus(evt)
        this.member = await QueueService.getQueueMembers();
        // console.log(member)
        this.io.emit('queueMember', { member: this.member });
        // const test = await QueueService.getQueueCallers()
        // console.log(test)
        // console.log(test[0]._lastStates)
        this.manager.action({
            Action: 'QueueStatus'
        });
        // console.log('handleQueueCallerAbandon', evt)
    }

    async handleQueueCallerLeave(evt) {
        try {
            // console.log('handleQueueCallerLeave', evt)
            QueueService.leaveQueueCaller(evt);
            this.queueParams = await QueueService.getQueueParams();
            // this.io.emit('queueCaller', { caller: caller });
            this.io.emit('queueParams', { queue: this.queueParams });
            QueueService.updateQueueCallerStatus(evt);
            // const test = await QueueService.getQueueCallers()
            // console.log(test)
            // console.log(test[0]._lastStates)
            this.manager.action({
                Action: 'QueueStatus'
            });
        } catch (error) {
            logger.error('Failed to process handle Queue Caller Leave:', error);
        }

        // console.log('queuecallerleave', QueueService.getQueueCallers())
        // console.log(QueueService.getQueueParams())
    }
    async handleAgentConnect(evt) {

        QueueService.updateQueueCallerStatus(evt);
        // const test = await QueueService.getQueueCallers()
        QueueService.updateQueueMemberStatus(evt)
        this.member = await QueueService.getQueueMembers();
        this.io.emit('queueMember', { member: this.member });
        // console.log(test)
        // console.log(test[0]._lastStates)
        // console.log('handleAgentConnect', evt)
    }

    async handleAgentComplete(evt) {
        QueueService.updateQueueCallerStatus(evt);
        // const test = await QueueService.getQueueCallers()
        // console.log(test)
        // console.log(test[0]._lastStates)
        this.manager.action({
            Action: 'QueueStatus'
        });
        console.log('handleAgentComplete', evt)
    }

    async handleAgentDump(evt) {
        console.log('handleAgentDump', evt)
    }

    async handleChanSpyStart(evt) {
        console.log('handleChanSpyStart', evt)
        callService.updateChanSpyStart(evt);
        PeerService.updateSpyerChannel(evt);
        this.sippeers = await PeerService.getPeers();
        this.io.emit('PeerStatus', { peers: this.sippeers });
        this.activeCall = await CallService.getActiveCall();
        this.io.emit('activeCall', { activeCall: this.activeCall });
    }
    async handleChanSpyStop(evt) {
        callService.updateChanSpyStop(evt);
        this.activeCall = await CallService.getActiveCall();
        this.io.emit('activeCall', { activeCall: this.activeCall });
        console.log('handleChanSpyStop', evt);
    }
    async handleChannelTalkingStart(evt) {
        console.log('handleChannelTalkingStart', evt)
    }

    async handleParkinglot(evt) {
        // console.log('handleParkinglot', evt)
        PeerService.updateParkinglot(evt)
    }
    async handleParkedCall(evt) {
        // console.log(evt)
        // await this.queueEvent('ParkedCall', evt);
        PeerService.updateParkeeChannel(evt);
        // this.park = await PeerService.getParkLots();
        // this.io.emit('ParkStatus', { park: this.park });
        // this.io.emit('updateParkedCall', {})
    }
    async handleParkedCallGiveUp(evt) {
        // console.log(evt)
        // await this.queueEvent('ParkedCallGiveUp', evt);
        PeerService.updateParkedCallGiveUp(evt);
        // this.park = await PeerService.getParkLots();
        // this.io.emit('ParkStatus', { park: this.park });
        // this.io.emit('updateParkedCall', {})
    }
    async handleParkedCallSwap(evt) {
        console.log(evt)
    }
    async handleParkedCallTimeOut(evt) {
        console.log(evt)
    }
    async handleHold(evt) {
        console.log(evt)
    }
    async handleUnhold(evt) {
        console.log(evt)
    }
    async handleDBGetResponse(evt) {
        // console.log(evt)
        if (evt.Event === 'DBGetResponse' && evt.Family === 'AMPUSER' && evt.Key.split('/')[1] === 'cidname')
            PeerService.updatePeerName(evt)
    }

    async handleCdr(evt) {
        console.log('handleCdr', evt)
    }


    async handleCoreShowChannel(evt) {
        console.log('handleCoreShowChannel', evt)
    }

    async handleDTMFBegin(evt) {
        console.log('handleDTMFBegin', evt)
    }

    async handleDeviceStateChange(evt) {
        console.log('handleDeviceStateChange', evt)
    }

    async handleDialBegin(evt) {
        console.log('handleDialBegin', evt)
    }

    async handleDialEnd(evt) {
        console.log('handleDialEnd', evt)
    }

    async handleDialState(evt) {
        console.log('handleDialState', evt)
    }

    async handleExtensionStateListComplete(evt) {
        console.log('handleExtensionStateListComplete', evt)
    }

    async asteriskPing(id) {
        try {
            this.manager.action({
                Action: 'Ping',
                ActionID: id
            });
        } catch (error) {
            logger.error('AMI service Ping error:', error);
        }
    }
    async reloadAction(id) {
        try {
            // this.manager.action({
            //     Action: 'Reload'
            // });
            // this.manager.action({
            //     Action: 'ExtensionStateList'
            // });
            this.manager.action({
                Action: 'Parkinglots',
            });
            // this.manager.action({
            //     Action: 'QueueStatus'
            // });
            // this.manager.action({
            //     Action: 'CoreShowChannels'
            // });
            // this.manager.action({
            //     Action: 'Sippeers'
            // });
            // this.manager.action({
            //     Action: 'ExtensionState',
            //     Exten: '200',
            //     Context: 'from-internal'
            // });
            // this.manager.action({
            //     Action: 'BridgeList'
            // });
            // this.manager.action({
            //     Action: 'DeviceStateList',
            // });
            // this.manager.action({
            //     Action: 'DeviceStateList',
            // });
            // this.manager.action({
            //     Action: 'QueueSummary',
            //     Queue: '6000'
            // });
            // this.manager.action({
            //     Action: 'QueueSummary',
            //     Queue: '6000'
            // });
            // this.manager.action({
            //     Action: 'VoicemailUsersList',
            // });
            const res = { 'notify_uid': id, 'message': `Reload System`, 'title': 'OdooPBX', 'sticky': false, 'warning': false }
            await odooService.sendEvent('asterisk_plus.settings', 'odoo_pbx_notify_1', res);
        } catch (error) {
            logger.error('AMI service Ping error:', error);
        }
    }
    async coreStatusAction(id) {
        try {
            this.manager.action({
                Action: 'CoreStatus',
                ActionID: id
            });
        } catch (error) {
            logger.error('AMI service Ping error:', error);
        }
    }

    async hangupAction(id) {
        try {
            this.manager.action({
                Action: 'Hangup',
                Channel: id
            });
        } catch (error) {
            logger.error('AMI service hangup Action error:', error);
        }
    }

    async parkAction(params) {
        try {
            this.manager.action({
                Action: 'Park',
                Channel: params.Channel,
                Channel2: params.AnnounceChannel,
                // TimeoutChannel: AnnounceChannel,
                // AnnounceChannel: AnnounceChannel,
                Timeout: 0,
                // Parkinglot: 'default',
                // ParkingSpace: '701',
            });
            this.manager.action({
                Action: 'ParkedCalls',
                ParkingLot: ''
            });


            // this.manager.action({
            //     Action: 'Redirect',
            //     Channel: Channel,
            //     Context: 'from-internal',
            //     Exten: '700',
            //     Priority: '1',
            // });

        } catch (error) {
            logger.error('AMI service Park Action error:', error);
        }

    }

    async unParkCall(params) {
        const path = 'C:\\Users\\alial\\AppData\\Local\\MicroSIP\\microsip.exe'
        const command = 'answer'
        try {
            this.manager.action({
                Action: 'Redirect',
                Channel: params.Channel,
                Exten: params.Exten,
                Priority: '1',
                Context: 'from-internal',
            });
            exec(`"${path}" /${command}`, (error, stdout, stderr) => {
                if (error) {
                    logger.error('SoftPhone command failed:', error);
                }
                logger.info('Auto Answer Process');
                // socket.emit('isAutoAnswer', { status: 'success' })
            });


        } catch (error) {
            logger.error('AMI service Un Park Action error:', error);
        }
    }

    async blindTransferAction(params) {
        try {
            this.manager.action({
                Action: 'BlindTransfer',
                Channel: params.Channel,
                'Context': 'from-internal-xfer',
                'Exten': params.Exten,
            });
        } catch (error) {
            logger.error('AMI service Blind Transfer Action error:', error);
        }
    }

    async setUserName(params) {
        try {
            this.manager.action({
                Action: 'DBPut',
                Family: 'AMPUSER',
                Key: params.cid + '/cidname',
                Val: params.name,
            });

        } catch (error) {
            logger.error('AMI service Set User Name Action error:', error);
        }
    }

    async setCallForwardAction(params) {
        try {
            this.manager.action({
                Action: 'DBPut',
                Family: params.Family,
                Key: params.Key,
                Val: params.Val,
            });
            this.manager.action({
                Action: 'DBPut',
                Family: 'AMPUSER',
                Key: params.Key + '/cfringtimer',
                Val: params.cfringtimer,
            });
            this.manager.action({
                Action: 'DBPut',
                Family: 'AMPUSER',
                Key: params.Key + '/ringtimer',
                Val: params.ringtimer,
            });
            if (params.Family == 'CF') {
                this.manager.action({
                    Action: 'DBDel',
                    Family: 'CFU',
                    Key: params.Key,
                });
                this.manager.action({
                    Action: 'DBDel',
                    Family: 'CFB',
                    Key: params.Key,
                });
            }
            if (params.Family == 'CFU') {
                this.manager.action({
                    Action: 'DBDel',
                    Family: 'CF',
                    Key: params.Key,
                });
                this.manager.action({
                    Action: 'DBDel',
                    Family: 'CFB',
                    Key: params.Key,
                });
            }
            if (params.Family == 'CFB') {
                this.manager.action({
                    Action: 'DBDel',
                    Family: 'CF',
                    Key: params.Key,
                });
                this.manager.action({
                    Action: 'DBDel',
                    Family: 'CFU',
                    Key: params.Key,
                });
            }
            PeerService.updateCallForward(params);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            this.io.emit('updateStatistic', {})

        } catch (error) {
            logger.error('AMI service Set Call Forward Action error:', error);
        }
    }

    async cancelCallForwardAction(params) {
        try {
            this.manager.action({
                Action: 'DBDel',
                Family: 'CF',
                Key: params.Key,
            });
            this.manager.action({
                Action: 'DBDel',
                Family: 'CFU',
                Key: params.Key,
            });
            this.manager.action({
                Action: 'DBDel',
                Family: 'CFB',
                Key: params.Key,
            });
            this.manager.action({
                Action: 'DBPut',
                Family: 'AMPUSER',
                Key: params.Key + '/ringtimer',
                Val: params.ringtimer,
            });
            // logger.info(`Disabled  [${params.Family}] to [${params.Key}] Action Process`);
            PeerService.updateCallForward(params);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            this.io.emit('updateStatistic', {})

        } catch (error) {
            logger.error('AMI service Cancel Call Forward Action error:', error);
        }

    }

    async setCwDndAction(params) {
        try {
            this.manager.action({
                Action: 'DBPut',
                Family: params.Family,
                Key: params.Key,
                Val: params.Val,
            });
            PeerService.updateCwDnd(params);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            logger.info(`Enabled [${params.Family}] to [${params.Key}] Action Process`);

        } catch (error) {
            logger.error('AMI service Set CW DND Action error:', error);
        }
    }
    async unSetCwDndAction(params) {
        try {
            this.manager.action({
                Action: 'DBDel',
                Family: params.Family,
                Key: params.Key,
            });
            PeerService.updateCwDnd(params);
            this.sippeers = await PeerService.getPeers();
            this.io.emit('PeerStatus', { peers: this.sippeers });
            logger.info(`Disabled  [${params.Family}] to [${params.Key}] Action Process`);

        } catch (error) {
            logger.error('AMI service UnSet CW DND Action error:', error);
        }
    }

    async originateCall(params) {
        try {
            this.manager.action({
                Action: 'Originate',
                Context: 'from-internal',
                Priority: '1',
                Timeout: params.Timeout,
                Channel: params.Channel,
                Exten: params.Exten,
                Async: 'true',
                EarlyMedia: 'true',
                CallerID: params.CallerID,
                ChannelId: params.ChannelId,
                OtherChannelId: params.OtherChannelId,
                Variable: params.Variable,
                Application: ''
            });
        } catch (error) {
            logger.error('AMI service Originate error:', error);
        }
    }
    async spyCall(params) {
        try {
            this.manager.action({
                Action: 'Originate',
                Async: 'true',
                EarlyMedia: 'true',
                Exten: params.Exten,
                CallerID: `<${params.CallerID} @ ${params.Data.split('/')[1].split(',')[0]}>`,
                Channel: params.Channel,
                ChannelId: params.ChannelId,
                Application: 'ChanSpy',
                Data: params.Data,
                Variable: params.Variable
            });
        } catch (error) {
            logger.error('AMI service spyCall error:', error);
        }
    }
    async lowercaseKeys(obj) {
        const newObj = {};
        for (let key in obj) {
            if (obj.hasOwnProperty(key)) {
                const lowerKey = key.toLowerCase();
                newObj[lowerKey] = obj[key];
            }
        }
        return newObj;
    }

}

module.exports = new AmiApiService();