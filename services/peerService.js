const logger = require('../utils/logger');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();
class PeerService {
    constructor() {
        this.peers = new Map();
        this.park = new Map();
    }

    async peerEntry(evt) {
        try {
            this.peers.set(evt.ObjectName, {
                ExtenNumber: evt.ObjectName,
                Peer: `${evt.Channeltype}/${evt.ObjectName}`,
                PeerStatus: evt.Status.startsWith('OK') ? 'Idle' : 'Offline',
                Ipaddress: (evt.IPaddress === '-none-' ? 'N/A' : evt.IPaddress + ':' + evt.IPport),
                ConnectedLineNum: '',
                ConnectedLineName: '',
                PeerChannel: '',
                StatusIcon: evt.Status.split(' ')[0] === 'OK' ? 'check-circle' : 'times-circle',
                ExtenName: '',
                AutoAnswerRingTime: '0',
                IsSpy: false,
                SpyerCallerIDNum: '',
                SpyerChannel: '',
                IsCallForward: false,
                CallForwardRingTimer: '0',
                CallForwardTimer: '0',
                CallForwardType: '',
                CallForwardDestination: '0',
                IsDND: false,
                IsCallWaiting: false,
                IsLoging: false,
                Wrapup: '0',
                VoiceMailBox: '',
                OldVoiceMailMessage: '0',
                NewVoiceMailMessage: '',
                LogInQueue: '',
            });
            // console.log(this.peers)
            logger.info(`PeerEntry[${evt.ActionID}] ${this.peers.get(evt.ObjectName).ExtenNumber} [${this.peers.get(evt.ObjectName).PeerStatus}] => Address: ${this.peers.get(evt.ObjectName).Ipaddress}`)
        } catch (err) {
            console.error('Error in handlePeerEntry', err)
        }
    }

    async updatePeerStatus(evt) {
        try {
            const peer = evt.Peer.split('/')[1];
            const statusMap = { 'Unregistered': 'Offline', 'Registered': 'Idle', 'Reachable': 'Idle' };
            const iconMap = { 'Unregistered': 'times-circle', 'UNKNOWN': 'check-times' };
            const sippeer = this.peers.get(peer);
            // console.log(sippeer)
            if (!sippeer) return;
            // if (sippeer) if (sippeer.status.includes('n')) return;;
            if (sippeer.PeerStatus == 'InUse') return;
            sippeer.Ipaddress = evt.Address || 'N/A'
            sippeer.PeerStatus = statusMap[evt.PeerStatus] || sippeer.PeerStatus;
            // sippeer.status = evt.PeerStatus;
            sippeer.StatusIcon = iconMap[evt.PeerStatus] || sippeer.StatusIcon;
            sippeer.PeerChannel = evt.Channel || '';
            this.peers.set(peer, sippeer)
            // console.log(sippeer)
            logger.info((`Peer:[${this.peers.get(peer).ExtenNumber}] status updated to [${evt.PeerStatus}] [${this.peers.get(peer).Ipaddress}]`))
        } catch (err) {
            console.log('Error in handlePeerStatus', err)
        }
    }

    async updateExtensionStatus(evt) {
        // console.log(evt)
        try {
            const peer = evt.Exten;
            const status_icon = {
                '0': 'Idle',
                '4': 'Offline',
                '1': 'InUse',
                '2': 'Busy',
                '8': 'Ringing',
                '9': 'InUseRinging',
                '16': 'Hold',
                '17': 'InUse&Hold'
            };
            const statusIcon = {
                '4': 'times-circle',
                '0': 'check-circle',
                '1': 'phone',
                '8': 'volume-control-phone',
                '9': 'volume-control-phone',
                '16': 'pause',
                '17': 'pause'
            }
            const sippeer = this.peers.get(peer);
            if (!sippeer) return;
            // console.log(evt)
            sippeer.PeerStatus = status_icon[evt.Status] || 'Offline';
            // sippeer.status = evt.StatusText;
            sippeer.StatusIcon = statusIcon[evt.Status] || 'times-circle';
            this.peers.set(peer, sippeer)
            logger.info((`Peer:[${this.peers.get(peer).Peer}] status updated to [${evt.StatusText}] [${this.peers.get(peer).Ipaddress}]`))
        } catch (err) {
            console.log('Error in updateExtensionStatus', err)

        }
    }


    async updateConnectedLineNum(evt) {
        try {
            const peer = evt.CallerIDNum;
            const sippeer = this.peers.get(peer);
            if (!sippeer) return;
            if (evt.ConnectedLineNum === '<unknown>') return;
            sippeer.ConnectedLineNum = evt.ConnectedLineNum;
            sippeer.PeerChannel = evt.Channel || '';
            // if (evt.Event === 'Hangup') {
            //     sippeer.connectedlinenum = '';
            //     sippeer.channel = '';

            // }
            this.peers.set(peer, sippeer)

        } catch (err) {
            console.log('Error in updateConnectedLineNum', err)
        }
    }

    async updateSpyerChannel(evt) {
        try {
            const peer = evt.SpyerCallerIDName;
            const sippeer = this.peers.get(peer);
            if (!sippeer) return;

            sippeer.ConnectedLineNum = evt.ConnectedLineNum;
            sippeer.PeerChannel = evt.SpyerChannel || '';

            // if (evt.Event === 'Hangup') {
            //     sippeer.connectedlinenum = '';
            //     sippeer.channel = '';

            // }
            this.peers.set(peer, sippeer)
        } catch (err) {
            console.log('Error in update Spyer Channel', err)
        }
    }

    async updateOriginateResponse(evt) {
        try {
            const peer = evt.Channel.split('-')[0].split('/')[1];
            const sippeer = this.peers.get(peer);
            if (!sippeer) return;

            sippeer.ConnectedLineNum = evt.Exten;
            sippeer.PeerChannel = evt.Channel || '';

            // if (evt.Event === 'Hangup') {
            //     sippeer.connectedlinenum = '';
            //     sippeer.channel = '';

            // }
            this.peers.set(peer, sippeer)
            console.log(sippeer)

        } catch (err) {
            console.log('Error in update Originate Response', err)
        }
    }
    async updateOnHangup(evt) {
        // console.log(evt)
        // const peer = evt.CallerIDNum;
        const peer = evt.Channel.split('-')[0].split('/')[1];
        const sippeer = this.peers.get(peer);
        if (!sippeer) return;
        sippeer.ConnectedLineNum = '';
        sippeer.PeerChannel = '';

    }
    async updateParkinglot(evt) {
        for (let i = Number(evt.StartSpace); i < Number(evt.StopSpace); i++) {
            this.park.set(String(i), {
                ParkNumber: String(i),
                Status: '',
                StatusText: '',
                Channel: '',
                ShortChannel: '',
                ParkeeExten: '',
            })
        }
        // console.log(this.park)
    }

    async updateParkinglotStatus(evt) {
        try {
            const park = this.park.get((evt.Exten));
            if (!park) return;
            // console.log(park)
            park.Status = evt.Status;
            park.StatusText = evt.StatusText;
            this.park.set(evt.Exten, park);
        } catch (error) {
            console.log('Error in updateParkinglotStatus', error)
        }
        // console.log(this.park)
    }
    async updateParkeeChannel(evt) {
        try {
            const park = this.park.get((evt.ParkingSpace));
            if (!park) return;
            park.Channel = evt.ParkeeChannel;
            park.ShortChannel = evt.ParkeeChannel.split('-')[0].split('/')[1];
            park.ParkeeExten = evt.ParkeeConnectedLineNum;

        } catch (error) {
            console.log('Error in updateParkeeChannel', error)
        }
    }
    async updateParkedCallGiveUp(evt) {
        try {
            const park = this.park.get((evt.ParkingSpace));
            if (!park) return;
            park.channel = '';
            park.ParkeeExten = '';
            park.ShortChannel = '';

        } catch (error) {
            console.log('Error in updateParkedCallGiveUp', error)
        }
    }
    async updateCwDnd(evt) {
        const sippeer = this.peers.get(evt.Key);
        if (!sippeer) return;
        if (evt.Action === 'SetCwDnd') {
            if (evt.Family === 'DND')
                sippeer.IsDND = true;
            if (evt.Family === 'CW')
                sippeer.IsCallWaiting = true;
            this.peers.set(evt.Key, sippeer)
        }
        if (evt.Action === 'UnSetCwDnd') {
            if (evt.Family === 'DND')
                sippeer.IsDND = false;
            if (evt.Family === 'CW')
                sippeer.IsCallWaiting = false;
            this.peers.set(evt.Key, sippeer)
        }
    }
    async updatePeerName(evt) {
        const peer = evt.Key.split('/')[0];
        this.peers.get(peer).ExtenName = evt.Val;
    }
    async updateCallForward(evt) {
        const sippeer = this.peers.get(evt.Key);
        if (!sippeer) return;
        if (evt.Action === 'SetCallForward') {
            sippeer.IsCallForward = true;
            sippeer.CallForwardType = evt.Family;
            sippeer.CallForwardDestination = evt.Val;
            sippeer.CallForwardRingTimer = evt.cfringtimer;
            sippeer.CallForwardTimer = evt.ringtimer;
            this.peers.set(evt.Key, sippeer)
        }
        if (evt.Action === 'CancelCallForward') {
            sippeer.IsCallForward = false;
            sippeer.CallForwardType = '';
            sippeer.CallForwardDestination = '';
            sippeer.CallForwardRingTimer = '';
            sippeer.CallForwardTimer = evt.ringtimer;
            this.peers.set(evt.Key, sippeer)
        }
        console.log(evt)
    }

    async getPeers() {

        return Array.from(this.peers.values()).map(peer => ({ ...peer }));
        // return Object.fromEntries(this.peers);
    }

    async getParkLots() {
        return Array.from(this.park.values()).map(park => ({ ...park }));
    }

    async getPeer(peerName) {
        if (this.peers.has(peerName)) {
            return this.peers.get(peerName);
        }
        return peer ? JSON.parse(peer) : null;
    }

    async getOnlinePeers() {
        // const peers = await this.getPeers();
        // return this.peers.filter(p => p.status === 'OK');


    }
    async setUserNameByExten() {
        try {
            this.peers.forEach(peer => {
                this.getUserNameByExtenSafe(process.env.ODOO_URL, '', peer.name)
                    .then(name => peer.username = name)
                    .catch(err => console.error('Error:', err));

            })

        } catch (err) {
            console.error('Error in setUserNameByExten', err)
        }

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

module.exports = new PeerService();