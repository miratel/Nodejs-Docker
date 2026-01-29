const axios = require('axios');
const logger = require('../utils/logger');
const { EventEmitter } = require('events');
const dotenv = require('dotenv');
dotenv.config();


class OdooService extends EventEmitter {
    constructor(config) {
        super();
        this.config = {
            url: process.env.ODOO_URL || 'http://192.168.0.171:8016',
            db: process.env.ODOO_DATABASE || '11',
            username: process.env.ODOO_USERNAME || 'odoovoip',
            password: process.env.ODOO_PASSWORD || 'odoovoip',
            ...config
        };
        this.uid = null;
        this.authenticated = false;
        this.pendingRequests = [];
        this.retryCount = 0;
        this.maxRetries = 5;
        this.status = 'disconnected';
        this.lastError = null;
    }

    getStatus() {
        return {
            status: this.status,
            authenticated: this.authenticated,
            lastError: this.lastError,
            pendingRequests: this.pendingRequests.length
        };
    }

    async getMetrics() {
        return {
            authenticated: this.authenticated,
            pendingRequests: this.pendingRequests.length,
            retryCount: this.retryCount,
            status: this.status
        };
    }

    async initialize() {
        this.status = 'initializing';
        try {
            await this.authenticate();
            setInterval(() => this.checkConnection(), 30000);
            this.status = 'connected';
        } catch (err) {
            this.status = 'error';
            this.lastError = err.message;
            logger.error('Odoo initialization failed:', err);
            throw err;
        }
    }

    async checkConnection() {
        try {
            if (!this.authenticated) {
                await this.authenticate();
                return;
            }

            // Simple ping to check if connection is alive
            await this.call('res.users', 'search_count', [[]], {}, { timeout: 5000 });
            // await this.call(model, method, [eventData], {}, { timeout: 15000 });

            this.status = 'connected';
        } catch (error) {
            this.status = 'disconnected';
            this.lastError = error.message;
            logger.error('Odoo connection check failed:', error.message);
            this.authenticated = false;
            await this.authenticate();
        }
    }

    async authenticate() {
        this.status = 'authenticating';

        try {
            const response = await axios.post(`${this.config.url}/web/session/authenticate`, {
                jsonrpc: "2.0",
                params: {
                    db: this.config.db,
                    login: this.config.username,
                    password: this.config.password,
                }
            }, { timeout: 10000 });

            if (response.data.result.uid) {
                this.uid = response.data.result.uid;
                this.authenticated = true;
                this.retryCount = 0;
                this.status = 'connected';
                this.lastError = null;
                logger.info('✅ Odoo authentication successful');
                this.processPendingRequests();
                return true;
            }
            throw new Error('Authentication failed - no UID received');
        } catch (error) {
            this.authenticated = false;
            this.status = 'error';
            this.lastError = error.message;

            logger.error('Odoo authentication error:', error.message);
            if (this.retryCount < this.maxRetries) {
                this.retryCount++;
                logger.info(`Retrying authentication (attempt ${this.retryCount})`);
                await new Promise(resolve => setTimeout(resolve, 5000));
                return this.authenticate();
            }
            throw error;
        }
    }

    async call(model, method, args = [], kwargs = {}, options = {}) {
        if (!this.authenticated) {
            return new Promise((resolve, reject) => {
                this.pendingRequests.push({ model, method, args, kwargs, options, resolve, reject });
                if (!this.authenticating) {
                    this.authenticating = true;
                    this.authenticate().finally(() => {
                        this.authenticating = false;
                    });
                }
            });
        }

        try {
            const response = await axios.post(`${this.config.url}/jsonrpc`, {
                jsonrpc: "2.0",
                method: "call",
                params: {
                    service: "object",
                    method: "execute_kw",
                    args: [this.config.db, this.uid, this.config.password, model, method, args, kwargs],
                },
                id: Math.floor(Math.random() * 1000)
            }, { timeout: options.timeout || 10000 });

            if (response.data.error) {
                throw new Error(response.data.error.message);
            }

            return response.data.result;
        } catch (error) {
            logger.error(`Odoo RPC error (${model}.${method}):`, error.message);

            if (error.response && error.response.status === 401) {
                this.authenticated = false;
                return this.call(model, method, args, kwargs, options); // Retry with auth
            }

            throw error;
        }
    }

    processPendingRequests() {
        while (this.pendingRequests.length > 0) {
            const { model, method, args, kwargs, options, resolve, reject } = this.pendingRequests.shift();
            this.call(model, method, args, kwargs, options)
                .then(resolve)
                .catch(reject);
        }
    }

    async sendEvent(model, method, eventData) {
        try {
            const result = await this.call(model, method, [eventData], {}, { timeout: 15000 });
            logger.debug(`Event sent to Odoo (${model}.${method})`);
            return result;
        } catch (error) {
            logger.error(`Failed to send event to Odoo (${model}.${method}):`, error.message);
            throw error;
        }
    }

    async getPBXconfiguration() {
        let servers = null
        try {
            // servers = await this.call('asterisk_plus.server', 'search', [[]], {}, { timeout: 5000 });
            // server = await this.call('asterisk_plus.server', 'browse', [[]], { servers }, { timeout: 5000 });
            servers = await this.sendEvent('asterisk_plus.server', 'get_pbx_configuration',)
        } catch (error) {
            console.log(error)
        }
        // console.log(servers)
        // console.log(server)
        // console.log(server.ami_host)
        return servers
    }

    async shutdown() {
        this.status = 'shutdown';
        // Cleanup any pending requests
        this.pendingRequests = [];
    }
}

// Singleton instance
const odooService = new OdooService();
odooService.initialize().catch(err => {
    logger.error('Failed to initialize Odoo service:', err);
});

module.exports = odooService;
