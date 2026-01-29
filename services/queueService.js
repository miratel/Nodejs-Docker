const logger = require('../utils/logger');
const callStatusMap = {
    '0': 'Offline',
    '1': 'OK',
    '2': 'InUse',
    '3': 'Busy',
    '4': 'InUse',
    '5': 'Offline',
    '6': 'Ringing',
    '7': 'InUseRinging',
    '8': 'Hold'
};

class QueueService {
    constructor() {
        this.queues = new Map();
        this.members = new Map();
        this.callers = new Map();
    }

    async updateQueueParams(evt) {
        try {
            const queueName = evt.Queue;
            if (queueName === 'default') return;
            this.queues.set(queueName, {
                queue: evt.Queue,
                max: evt.Max,                             //The name of the queue
                strategy: evt.Strategy,                   //The strategy of the queue.
                calls: evt.Calls,                         //The queue member's channel technology or location.
                holdtime: evt.Holdtime,                   //The queue's hold time
                talktime: evt.TalkTime,                   //The queue's talk time
                completed: evt.Completed,                 //The queue's completion time
                abandoned: evt.Abandoned,                 //The queue's call abandonment metric
                servicelevel: evt.ServiceLevel,
                servicelevelperf: evt.ServicelevelPerf,   //Primary service level performance metric.
                servicelevelperf2: evt.ServicelevelPerf2, //Secondary service level performance metric.
                agentmember: [],
                callers: [],
                timestamp: new Date().toISOString(),
            });

            logger.info(`Queue ${queueName} params updated`);
        } catch (err) {
            logger.error(`Queue params update failed: ${err.message}`);
        }
    }

    async updateQueueMember(evt) {
        try {
            const memberKey = evt.Name;
            let newMember = this.members.get(memberKey)
            if (!newMember) newMember = this._creatQueueMember(evt);

            this.members.set(memberKey, newMember)
            const agent = this.queues.get(evt.Queue)?.agentmember || [];
            if (!agent.includes(evt.Name)) agent.push(evt.Name)

            const queue = this.members.get(memberKey)?.queue || [];
            if (!queue.includes(evt.Queue)) queue.push(evt.Queue)

            logger.info(`Member ${evt.Name} updated in ${evt.Queue}`);
        } catch (err) {
            logger.error(`Member update failed: ${err.message}`);
        }
    }

    async addQueueMember(evt) {
        try {
            // const memberKey = `${evt.queue}:${evt.membername}`;
            const memberKey = evt.MemberName;
            const agent = this.queues.get(evt.Queue)?.agentmember || [];
            if (!agent.includes(evt.MemberName)) agent.push(evt.MemberName)

            if (!this.members.get(memberKey)) {
                const newMember = this._creatQueueMember(evt);
                this.members.set(memberKey, newMember)
                const queue = this.members.get(memberKey)?.queue || [];
                if (!queue.includes(evt.Queue)) queue.push(evt.Queue)
            };
            logger.info(`Agent[${evt.MemberName}] login In Queue[${evt.Queue}]`)
        } catch (err) {
            logger.error(`Member Added failed: ${err.message}`);
        }
    }
    async removedQueueMember(evt) {
        try {
            // console.log("888", evt)
            // const memberKey = `${evt.queue}:${evt.membername}`;
            const memberKey = evt.MemberName;
            let agent = this.queues.get(evt.Queue)?.agentmember || [];
            if (agent.includes(evt.MemberName)) this.queues.get(evt.Queue).agentmember = agent.filter(q => q !== evt.MemberName)

            const queue = this.members.get(memberKey)?.queue || [];
            // console.log(memberKey, this.members.get(memberKey))
            if (queue.includes(evt.Queue)) this.members.get(memberKey).queue = queue.filter(q => q !== evt.Queue)
            this.members.delete(memberKey)
            logger.warn(`Agent[${evt.MemberName}] logoff From Queue[${evt.Queue}]`)
        } catch (err) {
            logger.error(`Member Removed failed: ${err.message}`);
        }
    }
    async updateQueueMemberStatus(evt) {
        // console.log(evt)
        const member = evt.MemberName;
        if (!this.members.has(member)) return;
        const memberstatus = this.members.get(member);
        try {
            if (evt.Event !== 'AgentConnect') {
                memberstatus.event = evt.Event,
                    memberstatus.callstaken = evt.CallsTaken,
                    memberstatus.lastcall = evt.LastCall,
                    memberstatus.lastpause = evt.LastPause,
                    memberstatus.incall = '',
                    memberstatus.holdtime = '',
                    memberstatus.status = callStatusMap[evt.Status] || 'OffLine',
                    memberstatus.paused = evt.Paused,
                    memberstatus.pausedreason = evt.PausedReason,
                    memberstatus.wrapuptime = evt.Wrapuptime,
                    memberstatus.ringinuse = evt.Ringinuse || '1',
                    memberstatus.status = callStatusMap[evt.Status];


            };
            if (evt.Event === 'AgentConnect') {
                console.log(evt)
                memberstatus.incall = evt.CallerIDNum;
                memberstatus.holdtime = evt.HoldTime;
                memberstatus.ringtime = evt.RingTime;
            }
            if (evt.Event === 'AgentCalled') {
                console.log(evt)
                memberstatus.event = 'Agent Called' + (evt.MemberName);
                memberstatus.incall = evt.CallerIDNum;
                // memberstatus.holdtime = evt.HoldTime;
                // memberstatus.ringtime = evt.RingTime;
            }
            if (evt.Event === 'QueueCallerAbandon') {
                console.log(evt)
                memberstatus.event = 'Caller Abandon' + (evt.MemberName);
                memberstatus.incall = evt.CallerIDNum;
                // memberstatus.holdtime = evt.HoldTime;
                // memberstatus.ringtime = evt.RingTime;
            }
            if (evt.Event === 'AgentRingNoAnswer') {
                console.log(evt)
                memberstatus.event = 'Agent Ring NoAnswer' + (evt.MemberName);
                memberstatus.incall = evt.CallerIDNum;
                // memberstatus.holdtime = evt.HoldTime;
                // memberstatus.ringtime = evt.RingTime;
            }
            this.members.set(member, memberstatus)
            memberstatus.timestamp = new Date().toISOString();
            logger.info(`Agent[${evt.MemberName}] Queue[${evt.Queue}] Update Status[${memberstatus.status}]`);
        } catch (err) {
            logger.error(`Member Update Status failed: ${err.message}`);
        }


    }
    async updateQueueMemberAgentConnect(evt) {
        const member = evt.MemberName;
        if (!this.members.has(member)) return;
        const memberstatus = this.members.get(member);
        memberstatus.event = evt.Event;
        memberstatus.incall = evt.CallerIDNum;
        memberstatus.holdtime = evt.HoldTime;
        memberstatus.ringtime = evt.RingTime;
    }

    async addQueueCaller(evt) {
        // console.log('********', evt)
        try {
            const queueName = evt.Queue;
            const callers = this.queues.get(queueName)?.callers || [];
            if (!callers.includes(evt.CallerIDNum)) callers.push(evt.CallerIDNum)
            // const memberKey = `${evt.Queue}:${evt.CallerIDNum}`;
            // this.callers.set(memberKey, {
            //     position: evt.Position,
            //     callerId: evt.CallerIDNum,
            //     connectedlinenum: evt.Queue,
            //     waitTime: evt.Wait || '',
            //     uniqueId: evt.Uniqueid,
            //     channel: evt.Channel,
            // });
            logger.info(`Caller ${evt.CallerIDNum} Join to Queue ${queueName}`);
        } catch (err) {
            logger.error(`Queue caller Join failed: ${err.message}`);
        }
    }
    async leaveQueueCaller(evt) {

        try {
            const queueName = evt.Queue;
            const memberKey = `${evt.Queue}:${evt.CallerIDNum}`;
            const callers = this.queues.get(queueName)?.callers || [];
            if (callers.includes(evt.CallerIDNum)) this.queues.get(queueName).callers = callers.filter(caller => caller !== evt.CallerIDNum)

            if (this.callers.has(memberKey))
                this.callers.delete(memberKey);

            logger.info(`Caller ${evt.CallerIDNum} Leave Queue ${queueName}`);
        } catch (err) {
            logger.error(`Queue caller Leave failed: ${err.message}`);
        }
    }

    async updateQueueCallerStatus(evt) {
        const key = evt.Channel;
        let caller = this.callers.get(key)
        if (!caller) caller = this._creatQueueCaller(evt);

        // console.log(caller)
        caller._lastStates.push({
            Event: evt.Event,
            ConnectedLineNum: evt.ConnectedLineNum || '',
            ConnectedLineName: evt.ConnectedLineName || '',
            HoldTime: evt.HoldTime || '0',
            RingTime: evt.RingTime || '0',
            Interface: evt.Interface || '',
            MemberName: evt.MemberName || '',
            Position: evt.Position || '',
            OriginalPosition: evt.OriginalPosition || '',
            TalkTime: evt.TalkTime || '',
            timestamp: new Date().toISOString(),
        })
        this.callers.set(key, caller)
    }

    getQueues() {
        return Array.from(this.queues.values());
    }

    async getQueueParams() {
        // return Object.fromEntries(this.queues);
        // console.log(this.queues)
        return Array.from(this.queues.values()).map(queue => ({ ...queue }));
    }

    async getQueueMembers() {
        // console.log(this.members)
        // return Object.fromEntries(this.members);
        return Array.from(this.members.values()).map(member => ({ ...member }));
    }
    async getQueueCallers() {
        console.log(this.callers)
        // return Object.fromEntries(this.callers);
        return Array.from(this.callers.values()).map(caller => ({ ...caller }));
    }
    _creatQueueMember(evt) {
        return {
            event: evt.Event,
            name: evt.Name || evt.MemberName,
            membership: evt.Membership,
            penalty: evt.Penalty,
            location: evt.Location || evt.Interface,
            stateinterface: evt.StateInterface,
            callstaken: evt.CallsTaken,
            lastcall: evt.LastCall,
            lastpause: evt.LastPause,
            incall: '',
            holdtime: '',
            ringtime: '',
            talktime: '',
            status: callStatusMap[evt.Status] || 'OffLine',
            paused: evt.Paused,
            pausedreason: evt.PausedReason,
            wrapuptime: evt.Wrapuptime,
            timestamp: '',
            ringinuse: evt.Ringinuse || '1',
            queue: []
        };

    }

    _creatQueueCaller(evt) {
        return {
            Channel: evt.Channel,
            CallerIDNum: evt.CallerIDNum,
            CallerIDName: evt.CallerIDName,
            Exten: evt.Exten,
            Queue: evt.Queue,
            Position: evt.Position,
            Count: evt.Count,
            HoldTime: evt.HoldTime || '0',
            TalkTime: evt.TalkTime || '0',
            OriginalPosition: evt.OriginalPosition || '',
            _lastStates: [],
            timestamp: new Date().toISOString(),

        }
    }
}

module.exports = new QueueService();