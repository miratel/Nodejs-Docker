const AmiApiService = require('./ami_api');
const logger = require('../utils/logger');

class ActionError extends Error {
    constructor(message, statusCode = 400) {
        super(message);
        this.statusCode = statusCode;
    }
}

// Map actions to their respective service calls for clean, scalable code
const actionHandlers = {
    'Ping': (p, uid) => AmiApiService.asteriskPing(uid),
    'Reload': (p, uid) => AmiApiService.reloadAction(uid),
    'CoreStatus': (p, uid) => AmiApiService.coreStatusAction(uid),
    'Hangup': (p) => {
        if (!p.Channel) throw new ActionError('Missing required parameter: Channel');
        return AmiApiService.hangupAction(p.Channel);
    },
    'Originate': (p) => AmiApiService.originateCall(p),
    'OriginateSpy': (p) => AmiApiService.spyCall(p),
    'Park': (p) => AmiApiService.parkAction(p),
    'Redirect': (p) => AmiApiService.unParkCall(p),
    'BlindTransfer': (p) => AmiApiService.blindTransferAction(p),
    'SetCallForward': (p) => AmiApiService.setCallForwardAction(p),
    'CancelCallForward': (p) => AmiApiService.cancelCallForwardAction(p),
    'SetCwDnd': (p) => AmiApiService.setCwDndAction(p),
    'UnSetCwDnd': (p) => AmiApiService.unSetCwDndAction(p),
    'setUserName': (p) => AmiApiService.setUserName(p),
};

async function handleAgentAction(body) {
    if (body.fun === 'test.ping') {
        return { Response: 'Success', Ping: 'Pong' };
    }

    const { args, res_notify_uid } = body;
    if (!args || !args.Action) {
        throw new ActionError('Missing "args" or "Action" in request body');
    }

    const action = args.Action;
    logger.info(`Agent action requested: [${action}]`, args);

    const handler = actionHandlers[action];
    if (!handler) {
        throw new ActionError(`Action "${action}" is not supported.`, 404);
    }

    // Execute the handler
    await handler(args, res_notify_uid);

    if (AmiApiService.io) {
        const message = `"${action}" action processed.`
        AmiApiService.io.emit('sipUpdate', message);
    }

    return { status: 'success', message: `"${action}" action processed.` };
}

module.exports = { handleAgentAction };