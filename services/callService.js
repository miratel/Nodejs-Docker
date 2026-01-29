const logger = require('../utils/logger');
const axios = require('axios');
const dotenv = require('dotenv');
const PeerService = require('./peerService');
dotenv.config();

class CallService {
    constructor() {
        this.activecall = new Map();
    }
    async newCallChannel(evt) {

        if (!this._isValidEvent(evt)) return;
        try {
            // console.log(evt)
            const uniqueid = evt.Uniqueid;
            if (!this.activecall.has(uniqueid) && evt.Uniqueid === evt.Linkedid && evt.Exten !== 's') {
                const newCall = this._createNewCall(evt);
                this.activecall.set(uniqueid, newCall);
                logger.info(`New Call From ${evt.CallerIDNum} Start At ${new Date().toISOString()}`);
            };
            if (this.activecall.has(evt.Linkedid) && evt.Uniqueid != evt.Linkedid) {
                const linkedid = evt.Linkedid
                const call = this.activecall.get(linkedid)
                call.SecondaryChannel = evt.Channel; // Update target extension channel
                call.SecondaryUniqueid = evt.Uniqueid;     // Update linkedid for the main call 
                call.timestamp = new Date().toISOString();
                this.activecall.set(linkedid, call);
            }

        } catch (err) {
            console.error('Error In handleNewChannel:', err);
        }

    }
    async newCallState(evt) {
        if (!this._isValidEvent(evt)) return;
        // console.log(evt)
        try {
            const uniqueid = evt.Uniqueid;
            if (evt.Uniqueid === evt.Linkedid) {

                const call = this.activecall.get(uniqueid);
                if (call) {
                    // const connectedlinename = PeerService.peers.get(call.exten).username
                    call.PrimaryChannelState = evt.ChannelState;
                    call.PrimaryChannelStateDesc = evt.ChannelStateDesc;
                    call.ConnectedLineNum = evt.ConnectedLineNum;
                    // call.connectedlinename = connectedlinename || evt.ConnectedLineName;
                    call.ConnectedLineName = evt.ConnectedLineName;
                    this.activecall.set(uniqueid, call);
                }
            }
            else {
                const linkedid = evt.Linkedid;
                const call = this.activecall.get(linkedid);
                if (call) {
                    call.PrimaryChannelState = evt.ChannelState;
                    call.PrimaryChannelStateDesc = evt.ChannelStateDesc;
                    this.activecall.set(linkedid, call);
                }
            };
            // if (evt.context === 'from-queue') {
            //     const memberKey = `${evt.exten}:${evt.connectedlinenum}`;
            //     const caller = queueCallers.get(memberKey);
            //     if (caller) {
            //         caller.connectedlinenum = evt.calleridnum;
            //         caller.waitTime = evt.channelstatedesc;
            //         queueCallers.set(memberKey, caller)
            //     };
            // };
        } catch (err) {
            console.error('Error in handleNewState:', err);
        };
    }
    async originateResponse(evt) {
        if (evt.Response === 'Success') {
            const uniqueid = evt.Uniqueid;
            const newCall = this._createNewCall(evt);
            this.activecall.set(uniqueid, newCall);
        }
    }
    async callHangup(evt) {
        // console.log(evt)
        const uniqueid = evt.Uniqueid;
        try {
            if (this.activecall.has(uniqueid)) {
                this.activecall.delete(uniqueid);
                logger.info(`Call ${uniqueid} removed from active calls.`);
            }
        } catch (err) {
            console.error('Error in callHangup:', err);
        }

    }
    async updateChanSpyStart(evt) {
        try {
            const uniqueid = evt.SpyeeUniqueid;
            const call = this.activecall.get(uniqueid);
            if (call) {
                call.IsSpyer = true;
                call.SpyerCallerIDNum = evt.SpyerCallerIDNum;
                call.SpyerCallerIDName = evt.SpyerCallerIDName;
                call.SpyerUniqueid = evt.SpyerUniqueid;
                call.ChanSpyStartTimestamp = evt.Timestamp
                // console.log(call)
            }

            let call2 = null;
            for (const [key, value] of this.activecall.entries()) {
                if (value.linkedid === evt.SpyeeUniqueid) {
                    call2 = value;
                    break; // Stop after finding the first match
                }

            }
            if (call2) {
                call2.IsSpyer = true;
                call2.SpyerCallerIDNum = evt.SpyerCallerIDNum;
                call2.SpyerCallerIDName = evt.SpyerCallerIDName;
                call2.SpyerUniqueid = evt.SpyerUniqueid;
                call2.ChanSpyStartTimestamp = evt.Timestamp
            }


        } catch (error) {
            console.error('Error in update ChanSpy Start:', error);
        }
    }

    async updateChanSpyStop(evt) {
        try {
            // const uniqueid = evt.SpyerUniqueid;
            // const call = this.activecall.values().filter(call => call.spyer_uniqueid === evt.SpyerUniqueid)
            // const call = Object.fromEntries(this.activecall).map(call => call.spyer_uniqueid === evt.SpyerUniqueid)
            let call = null;
            for (const [key, value] of this.activecall.entries()) {
                if (value.SpyerUniqueid === evt.SpyerUniqueid) {
                    call = value;
                    break; // Stop after finding the first match
                }
            }
            if (!call) return
            call.IsSpyer = false;
            call.ChanSpyStopTimestamp = evt.Timestamp;
            // console.log(call)


        } catch (error) {
            console.error('Error in update ChanSpy Stop:', error);
        }
    }
    async handleBlindTransfer(evt) {
        if (evt.Event !== 'BlindTransfer') return;

        try {
            const transfererUniqueid = evt.TransfererUniqueid;
            const transfereeUniqueid = evt.TransfereeUniqueid;
            const linkedid = evt.TransfereeLinkedid;

            // Get the original call (transferee)
            const originalCall = this.activecall.get(transfereeUniqueid) ||
                this.activecall.get(linkedid);

            if (!originalCall) {
                logger.warn(`No active call found for transfer with uniqueid ${transfereeUniqueid}`);
                return;
            }

            // Update the original call with transfer information
            originalCall.IsTransfer = true;
            originalCall.TransferType = 'blind';
            originalCall.TransfererCallerIDNum = evt.TransfererCallerIDNum;
            originalCall.TransfererCallerIDName = evt.TransfererCallerIDName;
            originalCall.TransferTarget = evt.Extension;
            originalCall.TransferTimestamp = evt.Timestamp;

            // Remove the transferer's channel from active calls
            if (this.activecall.has(transfererUniqueid)) {
                this.activecall.delete(transfererUniqueid);
            }

            // Update the transferee's call with new connection info
            if (this.activecall.has(transfereeUniqueid)) {
                const call = this.activecall.get(transfereeUniqueid);
                call.ConnectedLineNum = evt.TransfereeConnectedLineNum;
                call.ConnectedLineName = evt.TransfereeConnectedLineName;
                this.activecall.set(transfereeUniqueid, call);
            }

            logger.info(`Call transferred from ${originalCall.calleridnum} to ${evt.Extension} by ${evt.TransfererCallerIDNum}`);

        } catch (err) {
            console.error('Error in handleBlindTransfer:', err);
        }
    }
    async handleNewConnectedLine(evt) {
        // This handles updates when the connected line changes (like after transfer)
        if (!this._isValidEvent(evt)) return;
        // console.log('**********', evt)
        try {
            const uniqueid = evt.Uniqueid;
            const linkedid = evt.Linkedid;

            // Check both the uniqueid and linkedid since the call might be keyed by either
            const call = this.activecall.get(uniqueid) || this.activecall.get(linkedid);
            // console.log('*********', call)
            if (call) {
                call.ConnectedLineNum = evt.ConnectedLineNum;
                call.ConnectedLineName = evt.ConnectedLineName;
                // if (evt.Exten !== 's') call.exten = evt.Exten;
                call.timestamp = new Date().toISOString();

                if (uniqueid !== linkedid) {
                    // This is the target extension's channel
                    call.SecondaryChannel = evt.Channel;
                }

                this.activecall.set(call.PrimaryUniqueid, call);
            }
        } catch (err) {
            console.error('Error in handleNewConnectedLine:', err);
        }
    }

    async getActiveCall() {
        // return Object.fromEntries(this.activecall);
        // console.log(this.activecall)
        return Array.from(this.activecall.values()).map(call => ({ ...call }));
    }

    _createNewCall(evt) {
        return {
            PrimaryChannel: evt.Channel,
            PrimaryUniqueid: evt.Uniqueid,
            SecondaryUniqueid: evt.Linkedid || '',
            PrimaryChannelState: evt.ChannelState,
            PrimaryChannelStateDesc: evt.ChannelStateDesc,
            CallerIDNum: evt.CallerIDNum,
            CallerIDName: evt.CallerIDName,
            ConnectedLineNum: evt.ConnectedLineNum,
            ConnectedLineName: evt.ConnectedLineName,
            Exten: evt.Exten,
            Direction: '',
            SecondaryChannel: '', // Target extension channel (updated later)
            IsSpyer: false,
            SpyerChannel: '',
            SpyerCallerIDNum: '',
            SpyerCallerIDName: '',
            SpyerUniqueid: '',
            ChanSpyStartTimestamp: '',
            ChanSpyStopTimestamp: '',
            ChanSpyType: '',
            IsTransfer: false,
            TransferType: '', // 'blind' or 'attended'
            TransfererCallerIDNum: '',
            TransfererCallerIDName: '',
            TransferTarget: '',
            TransferTimestamp: '',
            timestamp: new Date().toISOString(),
        };
    }
    _isValidEvent(evt) {
        return (
            evt.Uniqueid &&
            evt.Channel &&
            !evt.Exten.includes('*')
            // && !evt.Exten.includes('@')
        );
    }
    async getUserNameByExtenSafe(odooUrl, dbName, exten) {
        try {
            const response = await axios.get(`${odooUrl}/asterisk_plus/get_user_name_by_exten`, {
                params: { db: dbName, exten: exten },
                timeout: 5000 // 5 second timeout
            });

            // Handle possible responses
            if (response.data === 'db not specified') {
                throw new Error('Database parameter missing');
            } else if (response.data === 'db does not exist') {
                throw new Error('Specified database does not exist');
            } else if (response.data === 'Extension not specified') {
                throw new Error('Extension parameter missing');
            } else if (response.data.includes('not allowed')) {
                throw new Error('IP address not whitelisted');
            }

            return response.data; // Returns name or empty string
        } catch (error) {
            if (error.code === 'ECONNABORTED') {
                throw new Error('Request timeout');
            }
            throw error;
        }
    }

}

module.exports = new CallService();