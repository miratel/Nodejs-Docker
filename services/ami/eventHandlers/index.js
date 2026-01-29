const callEvents = require('./callEvents');
const peerEvents = require('./peerEvents');
const queueEvents = require('./queueEvents');
const otherEvents = require('./otherEvents');

function registerEventHandlers(manager, io) {
    const eventModules = [callEvents, peerEvents, queueEvents, otherEvents];
    
    eventModules.forEach(module => {
        // The module should export a function that takes manager and io
        // and registers its specific handlers.
        if (typeof module.register === 'function') {
            module.register(manager, io);
        }
    });
}

module.exports = { registerEventHandlers };