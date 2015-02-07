/**
 * Copyright © 2013 Cask Data, Inc.
 *
 * Base server used for developer and enterprise editions. This provides common functionality to
 * set up a node js server and define routes. All custom functionality to an edition
 * must be placed under the server file inside the edition folder.
 */

var express = require('express'),
  Int64 = require('node-int64'),
  fs = require('fs'),
  log4js = require('log4js'),
  path = require('path'),
  promise = require('q'),
  http = require('http'),
  https = require('https'),
  promise = require('q'),
  lodash = require('lodash'),
  request = require('request');


var Api = require('./api'),
    configParser = require('./configParser');
    Env = require('./env');

/**
 * Generic web app server. This is a base class used for creating different editions of the server.
 * This provides base server functionality, logging, and routes setup.
 * @param {string} dirPath from where module is instantiated. This is used becuase __dirname
 * defaults to the location of this module.
 * @param {string} logLevel log level {TRACE|INFO|ERROR}
 * @param {boolean} https whether to use https for requests.
 */
var WebAppServer = function(dirPath, logLevel, loggerType, mode) {
  this.dirPath = dirPath;
  this.logger = this.getLogger('console', loggerType);
  this.isDefaultConfig = false;
  this.LOG_LEVEL = logLevel;
  process.on('uncaughtException', function (err) {
    this.logger.info('Uncaught Exception', err);
  }.bind(this));
  this.extractConfig(mode, "cdap", false)
      .then(function () {
        this.setUpServer();
      }.bind(this));
};

WebAppServer.prototype.extractConfig = configParser.extractConfig;

/**
 * Thrift API service.
 */
WebAppServer.prototype.Api = Api;
/**
 * API version.
 */
WebAppServer.prototype.API_VERSION = 'v2';


/**
 * Express app framework.
 */
WebAppServer.prototype.app = express();

/**
 * Config.
 */
WebAppServer.prototype.config = {};

/**
 * Configuration file pulled in and set.
 */
WebAppServer.prototype.configSet = false;

/**
 * How often to call security server in case no response is found.
 */
WebAppServer.prototype.SECURITY_TIMER = 1000;

/**
 * Globals
 */
var PRODUCT_VERSION, PRODUCT_ID, PRODUCT_NAME, IP_ADDRESS;

/**
 * Security Globals
 */
var SECURITY_ENABLED, AUTH_SERVER_ADDRESSES;

WebAppServer.prototype.setUpServer = function setUpServer(configuration) {
  this.setAttributes();
  this.Api.configure(this.config, this.apiKey || null);
  this.launchServer();
}

WebAppServer.prototype.setAttributes = function setCommonAttributes() {
  if (this.config['ssl.enabled'] === "true") {
      this.lib = https;
      if (this.config["dashboard.ssl.disable.cert.check"] === "true") {
        /*
          We use mikeal/request library to make xhr request to cdap server.
          In a ssl enabled environment where cdap server uses a self-signed certificate
          node server will fail to connect to cdap server as it is self-signed.
          This environment variable enables that.

          The github issue in relation to this is : https://github.com/mikeal/request/issues/418

          Could not find nodejs doc that discusses about this variable.
        */
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
      }
    } else {
      this.lib = http;
    }
    this.apiKey = this.config.apiKey;
    this.version = this.config.version;
    this.configureExpress();
    this.setCookieSession(this.cookieName, this.secret);
}

WebAppServer.prototype.launchServer = function() {
  var options = this.configureSSL() || {};
  this.server = this.getServerInstance(options, this.app);
  this.setEnvironment(this.productId, this.productName, this.version, this.startServer.bind(this));
}

WebAppServer.prototype.configureSSL = function () {
  var options = {},
      key,
      cert;
  if (this.config['ssl.enabled'] === "true") {
    key = this.securityConfig['dashboard.ssl.key'],
    cert = this.securityConfig['dashboard.ssl.cert'];
    try {
      options = {
        key: fs.readFileSync(key),
        cert: fs.readFileSync(cert),
        requestCert: false,
        rejectUnauthorized: false
      };
      this.config['dashboard.bind.port'] = this.config['dashboard.ssl.bind.port'];
    } catch (e) {
      this.logger.info("Error Reading ssl key/certificate: ", e);
    }
  }
  return options;
}


/**
 * Determines security status. Continues until it is able to determine if security is enabled if
 * CDAP is down.
 * @param  {Function} callback to call after security status is determined.
 * TODO: Reimplement logic to check if security is enabled, CDAP-17 
 */
WebAppServer.prototype.setSecurityStatus = function (callback) {
  var self = this;

  var path = '/' + this.API_VERSION + '/ping',
      url;
  this.routerBindAddress = this.config['router.server.address'];
  if (this.config['ssl.enabled'] === "true") {
    this.routerBindPort = this.config['router.ssl.server.port'];
    this.transferProtocol = "https://";
    url = 'https://' + this.config['router.server.address'] + ':' + this.config['router.ssl.server.port'] + path;
  } else {
    this.routerBindPort = this.config['router.bind.port'];
    this.transferProtocol = "http://";
    url = 'http://' + this.config['router.server.address'] + ':' + this.config['router.bind.port'] + path;
  }
  var interval = setInterval(function () {
    self.logger.info('Calling security endpoint: ', url);
    request({
      method: 'GET',
      url: url,
      rejectUnauthorized: false,
      requestCert: true,
      agent: false
    }, function (err, response, body) {
      // If the response is a 401 and contains "auth_uri" as part of the body, CDAP security is enabled.
      // On other response codes, and when "auth_uri" is not part of the body, CDAP security is disabled.
      if (!err && response) {
        clearInterval(interval);
        if (body) {
          if (response.statusCode === 401 && JSON.parse(body).auth_uri) {
            SECURITY_ENABLED = true;
            AUTH_SERVER_ADDRESSES = JSON.parse(body).auth_uri;
          } else {
            SECURITY_ENABLED = false;
          }
        } else {
          SECURITY_ENABLED = false;
        }
        self.logger.info('Security configuration found. Security is enabled: ', SECURITY_ENABLED);
        if (typeof callback === 'function') {
          callback();
        }
      }
    });
  }, self.SECURITY_TIMER);
};

/**
 * Randomly picks and returns an Authentication service to connect to.
 */
WebAppServer.prototype.getAuthServerAddress = function() {
  if (AUTH_SERVER_ADDRESSES.length === 0) {
    return null;
  }
  return AUTH_SERVER_ADDRESSES[Math.floor(Math.random() * AUTH_SERVER_ADDRESSES.length)];
};

/**
 * Sets version if a version file exists.
 */
WebAppServer.prototype.setEnvironment = function(id, product, version, callback) {

  version = version ? version.replace(/\n/g, '') : 'UNKNOWN';

  PRODUCT_ID = id;
  PRODUCT_NAME = product;
  PRODUCT_VERSION = version;

  this.logger.info('PRODUCT_ID', PRODUCT_ID);
  this.logger.info('PRODUCT_NAME', PRODUCT_NAME);
  this.logger.info('PRODUCT_VERSION', PRODUCT_VERSION);

  // Check security status only when a callback is passed.
  // This is a minor hack since sandboxes call this function twice, but we need it to check
  // Security status only on the call with callbacks.
  if (typeof callback === 'function') {
    this.setSecurityStatus(function() {
      Env.getAddress(function (address) {
        IP_ADDRESS = address;
        callback(PRODUCT_VERSION, address);
      }.bind(this));
    }.bind(this));
  }
};

/**
 * Configures logger.
 * @param {string} opt_appenderType log4js appender type.
 * @param {string} opt_logger log4js logger name.
 * @return {Object} instance of logger.
 */
WebAppServer.prototype.getLogger = function(opt_appenderType, opt_loggerName) {
  var appenderType = opt_appenderType || 'console';
  var loggerName = opt_loggerName || 'Developer UI';
  log4js.configure({
    appenders: [
      {type: appenderType}
    ]
  });
  var logger = log4js.getLogger(loggerName);
  logger.setLevel(this.LOG_LEVEL);
  return logger;
};

/**
 * Configures express server.
 */
WebAppServer.prototype.configureExpress = function() {

  this.app.use(express.bodyParser());

  // Workaround to make static files work on cloud.
  if (fs.existsSync(this.dirPath + '/../client/')) {
    this.app.use(express['static'](this.dirPath + '/../client/'));
  } else {
    this.app.use(express['static'](this.dirPath + '/../../client/'));
  }

};

/**
 * Sets a session in express and assigns a cookie identifier.
 * @param {string} cookieName Name of the cookie.
 * @param {string} secret cookie secret.
 */
WebAppServer.prototype.setCookieSession = function(cookieName, secret) {
  this.app.use(express.cookieParser());
  this.app.use(express.session({secret: secret, key: cookieName}));
};

/**
 * Creates http server based on app framework.
 * Currently works only with express.
 * @param {Object} app framework.
 * @return {Object} instance of the http server.
 */
WebAppServer.prototype.getServerInstance = function(options, app) {
  if (Object.keys(options).length > 0) {
    return this.lib.createServer(options, app);
  }

  return this.lib.createServer(app);
};

WebAppServer.prototype.checkAuth = function(req, res, next) {
  if (!('token' in req.cookies)) {
    req.cookies.token = '';
  }
  next();
};

/**
 * Binds individual expressjs routes. Any additional routes should be added here.
 */
WebAppServer.prototype.bindRoutes = function() {

  var self = this;
  // Check to see if config is set.
  if(!this.configSet) {
    this.logger.info('Configuration file not set ', this.config);
    return false;
  }

  var availableMetrics = {
    'App': [
      { name: 'Events Collected', path: '/system/apps/{id}/collect.events' },
      { name: 'Busyness', path: '/system/apps/{id}/process.busyness' },
      { name: 'Bytes Stored', path: '/system/apps/{id}/store.bytes' },
      { name: 'Queries Served', path: '/system/apps/{id}/query.requests' }
    ],
    'Stream': [
      { name: 'Events Collected', path: '/system/streams/{id}/collect.events' },
      { name: 'Bytes Collected', path: '/system/streams/{id}/collect.bytes' },
      { name: 'Reads per Second', path: '/system/streams/{id}/collect.reads' }
    ],
    'Flow': [
      { name: 'Busyness', path: '/system/apps/{parent}/flows/{id}/process.busyness' },
      { name: 'Events Processed', path: '/system/apps/{parent}/flows/{id}/process.events.processed' },
      { name: 'Bytes Processed', path: '/system/apps/{parent}/flows/{id}/process.bytes' },
      { name: 'Errors per Second', path: '/system/apps/{parent}/flows/{id}/process.errors' }
    ],
    'Mapreduce': [
      { name: 'Completion', path: '/system/apps/{parent}/mapreduce/{id}/process.completion' },
      { name: 'Records Processed', path: '/system/apps/{parent}/mapreduce/{id}/process.entries' }
    ],
    'Dataset': [
      { name: 'Bytes per Second', path: '/system/datasets/{id}/dataset.store.bytes' },
      { name: 'Reads per Second', path: '/system/datasets/{id}/dataset.store.reads' }
    ],
    'Procedure': [
      { name: 'Requests per Second', path: '/system/apps/{parent}/procedures/{id}/query.requests' },
      { name: 'Failures per Second', path: '/system/apps/{parent}/procedures/{id}/query.failures' }
    ]

  };

  this.app.get('/rest/metrics/system/:type', function (req, res) {

    var type = req.params.type;
    res.send(availableMetrics[type] || []);

  });

  /**
   * User Metrics Discovery
   */
  this.app.get('/rest/metrics/user/*', this.checkAuth, function (req, res) {

    var path = req.url.slice(18);

    self.logger.trace('User Metrics', path);

    var options = {
      host: self.routerBindAddress,
      port: self.routerBindPort,
      method: 'GET',
      path: '/' + self.API_VERSION + '/metrics/available' + path,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    var request = self.lib.request(options, function(response) {
      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        try {
          data = JSON.parse(data);
          res.send({ result: data, error: null });
        } catch (e) {
          self.logger.error('Parsing Error', data);
          res.send({ result: null, error: data });
        }

      });
    });

    request.on('error', function(e) {

      res.send({
        result: null,
        error: {
          fatal: 'UserMetricsService: ' + e.code
        }
      });

    });

    request.end();

  });

  /**
   * Enable testing.
   */
  this.app.get('/test', function (req, res) {
    res.sendfile(path.resolve(__dirname + '../../../client/test/TestRunner.html'));
  });

  /*
   * REST DELETE handler.
   */
  this.app.del('/rest/*', this.checkAuth, function (req, res) {

    var url = self.routerBindAddress + ':' + self.routerBindPort;
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);

    request({
      method: 'DELETE',
      url: self.transferProtocol + path,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    }, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not DELETE', path, body, error,  response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(response.statusCode,
            'Unable to connect to the CDAP Gateway. Please check your configuration.');
        } else {
          res.send(response.statusCode, body || error || response.statusCode);
        }
      }
    });

  });

  /*
   * REST PUT handler.
   */
  this.app.put('/rest/*', this.checkAuth, function (req, res) {
    var url = self.routerBindAddress + ':' + self.routerBindPort;
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);
    var opts = {
      method: 'PUT',
      url: self.transferProtocol + path,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    if (req.body) {
      opts.body = req.body.data;
      if (typeof opts.body === 'object') {
        opts.body = JSON.stringify(opts.body);
      }
      opts.body = opts.body || '';
    }
    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not PUT to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the CDAP Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /**
   * Promote handler.
   */
  this.app.post('/rest/apps/:appId/promote', this.checkAuth, function (req, res) {
    var url = self.routerBindAddress + ':' + self.routerBindPort;
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);
    var opts = {
      method: 'POST',
      url: self.transferProtocol + path,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    if (req.body) {
      opts.body = {
        hostname: req.body.hostname
      };
      opts.body = JSON.stringify(opts.body) || '';
      opts.headers = {
        'X-ApiKey': req.body.apiKey,
        'Authorization': 'Bearer ' + req.cookies.token
      };
    }

    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not POST to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the CDAP Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /*
   * REST POST handler.
   */
  this.app.post('/rest/*', this.checkAuth, function (req, res) {
    var url = self.routerBindAddress + ':' + self.routerBindPort;
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);
    var opts = {
      method: 'POST',
      url: self.transferProtocol + path,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    if (req.body) {
      opts.body = req.body.data;
      if (typeof opts.body === 'object') {
        opts.body = JSON.stringify(opts.body);
      }
      opts.body = opts.body || '';
    }

    request(opts, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not POST to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the CDAP Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.body || response.statusCode);
        }
      }
    });
  });

  /*
   * REST GET handler.
   */
  this.app.get('/rest/*', this.checkAuth, function (req, res) {

    var url = self.routerBindAddress + ':' + self.routerBindPort;
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);

    var opts = {
      method: 'GET',
      url: self.transferProtocol + path,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not GET', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the CDAP Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /*
   * Metrics Handler
   */
  this.app.post('/metrics', this.checkAuth, function (req, res) {

    var pathList = req.body;
    var accountID = 'developer';

    self.logger.trace('Metrics ', pathList);

    if (!pathList) {
      self.logger.error('No paths posted to Metrics.');
      res.send({
        result: null,
        error: {
          fatal: 'MetricsService: No paths provided.'
        }
      });
      return;
    }

    var content = JSON.stringify(pathList);

    var options = {
      host: self.routerBindAddress,
      port: self.routerBindPort,
      path: '/' + self.API_VERSION + '/metrics',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': content.length,
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    var request = self.lib.request(options, function(response) {
      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        try {
          data = JSON.parse(data);
          res.send({ result: data, error: null });
        } catch (e) {
          self.logger.error('Parsing Error', data);
          res.send({ result: null, error: 'Parsing Error' });
        }

      });
    });

    request.on('error', function(e) {

      res.send({
        result: null,
        error: {
          fatal: 'MetricsService: ' + e.code
        }
      });

    });

    request.write(content);
    request.end();

  });

  /**
   * Upload an Application archive.
   */
  this.app.post('/upload/:file', this.checkAuth, function (req, res) {
    var url = self.transferProtocol + self.routerBindAddress + ':' +
      self.routerBindPort + '/' + self.API_VERSION + '/apps';

    var opts = {
      method: 'POST',
      url: url,
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    var x = request.post(opts);
    req.pipe(x);
    x.pipe(res);
  });

  this.app.post('/unrecoverable/reset', this.checkAuth, function (req, res) {

    var host = self.routerBindAddress + ':' + self.routerBindPort;

    var opts = {
      method: 'POST',
      url: self.transferProtocol + host + '/' + self.API_VERSION + '/unrecoverable/reset',
      headers: {
        'X-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    request(opts, function (error, response, body) {

      if (error || response.statusCode !== 200) {
        res.send(400, body);
      } else {
        res.send('OK');
      }

    });

  });

  this.app.get('/environment', function (req, res) {

    var environment = {
      'product_id': PRODUCT_ID,
      'product_name': PRODUCT_NAME,
      'product_version': PRODUCT_VERSION,
      'ip': IP_ADDRESS,
      'explore_enabled' : self.config['explore.enabled'],
      'security_enabled': SECURITY_ENABLED
    };

    if (req.session.account_id) {

      environment.account = {
        account_id: req.session.account_id,
        name: req.session.name
      };

    }

    if (process.env.NODE_ENV !== 'production') {
      environment.credential = self.Api.credential;
      environment.nux = false;
      // Disabling Nux
      // TODO: Enable NUX with new tutorial, see CDAP-22
      /* 
      if (!fs.existsSync('./.nux_dashboard')) {
        environment.nux = true;
      } else {
        environment.nux = false;
      }
      */

    } else {
      if ('info' in self.config) {
        environment.cluster = self.config.info;
      }
    }

    res.send(environment);

  });

  this.app.get('/nux_complete', function (req, res) {

    fs.openSync('./.nux_dashboard', 'w');
    res.send('OK');

  });

  // Security endpoints.
  this.app.get('/getsession', function (req, res) {
    var headerOpts = {};
    var token = '';
    if ('token' in req.cookies && req.cookies.token !== '') {
      headerOpts['Authorization'] = "Bearer " + req.cookies.token;
      token = req.cookies.token;
    }

    var options = {
      host: self.routerBindAddress,
      port: self.routerBindPort,
      path: '/' + self.API_VERSION + '/ping',
      method: 'GET',
      headers: headerOpts
    };

    var request = self.lib.request(options, function (response) {
        var data = '';
        response.on("data", function(chunk) {
            data += chunk;
        });

        response.on('end', function () {
          if (response.statusCode === 401) {
            AUTH_SERVER_ADDRESSES  = JSON.parse(data).auth_uri;
            token = '';
            res.clearCookie('token');
          }
          res.send({token: token});
        });
    });
    request.end();

  });

  this.app.post('/validatelogin', function (req, res) {
      var auth_uri = self.getAuthServerAddress();
      if (auth_uri === null) {
        res.send(500, "No Authentication service to connect to was found.");
      } else {
        var post = req.body;
        var options = {
          url: auth_uri,
          auth: {
            user: post.username,
            password: post.password
          },
          rejectUnauthorized: false,
          requestCert: true,
          agent: false
        }
        request(options, function (nerr, nres, nbody) {
          if (nerr || nres.statusCode !== 200) {
            res.send(401);
          } else {
            res.send(200);
          }
        });
      }
   });

   this.app.post('/login', function (req, res) {
      req.session.regenerate(function () {
        var auth_uri = self.getAuthServerAddress();
        if (auth_uri === null) {
          res.send(500, "No Authentication service to connect to was found.");
        } else {
          var post = req.body;
          var options = {
            url: auth_uri,
            auth: {
              user: post.username,
              password: post.password
            },
            rejectUnauthorized: false,
            requestCert: true,
            agent: false
          }

          request(options, function (nerr, nres, nbody) {
            if (nerr || nres.statusCode !== 200) {
              res.locals.errorMessage = "Please specify a valid username and password.";
              res.redirect('/#/login');
            } else {
              var nbody = JSON.parse(nbody);
              res.cookie('token', nbody.access_token, { httpOnly: true } );
              res.redirect('/#/overview');
            }
          });
        }
      });
   });

  this.app.get('/logout', this.checkAuth, function (req, res) {
      req.session.regenerate(function () {
        res.clearCookie('token');
        res.redirect('/#/login');
      })
   });

  this.app.post('/accesstoken', function (req, res) {
     req.session.regenerate(function () {
       var auth_uri = self.getAuthServerAddress();
       auth_uri = auth_uri.slice(0, -5);
       auth_uri += "extendedtoken";
       if (auth_uri === null) {
         res.send(500, "No Authentication service to connect to was found.");
       } else {
         var post = req.body;
         var options = {
           url: auth_uri,
           auth: {
             user: post.username,
             password: post.password
           },
           rejectUnauthorized: false,
           requestCert: true,
           agent: false
         }

         request(options, function (nerr, nres, nbody) {
           if (nerr || nres.statusCode !== 200) {
             res.send(400, "Please specify a valid username and password.");
           } else {
             var nbody = JSON.parse(nbody);
             res.send(nbody);
           }
         });
       }
     });
  });

  this.app.get('/download-access-token/*', function (req, res) {
    var accessToken = req.params[0];
    res.attachment('.cdap.accesstoken');
    res.end(accessToken, 'utf-8');
  });

  this.app.get('/download-query-results/*', function (req, res) {
    var query_handle = req.params[0];

    var path = self.routerBindAddress + ':' + self.routerBindPort
                + '/' + self.API_VERSION + '/data/explore/queries/' + query_handle + '/download';
    var opts = {
      method: 'POST',
      url: self.transferProtocol + path,
      headers: {
        'X-Continuuity-ApiKey': req.session ? req.session.api_key : '',
        'Authorization': 'Bearer ' + req.cookies.token
      }
    };

    request(opts, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        res.attachment('results_' + query_handle);
        res.end(body, 'utf-8');
      } else {
        self.logger.error('Could not PUT to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the CDAP Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /**
   * Check for new version.
   */
  this.app.get('/version', function (req, res) {
    var options = {
      host: 'docs.cask.co',
      path: '/cdap/version'
    };

    var request = self.lib.request(options, function(response) {
      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        data = data.replace(/\n/g, '');

        res.send({
          current: PRODUCT_VERSION,
          newest: data
        });

      });
    });

    request.end();

  });

  /**
   * Get a list of push destinations.
   */
  this.app.get('/destinations', function  (req, res) {

    fs.readFile(self.dirPath + '/.credential', 'utf-8', function (error, result) {

      res.on('error', function (e) {
        self.logger.trace('/destinations', e);
      });

      if (error) {

        res.write('false');
        res.end();

      } else {

        var options = {
          host: self.config['accounts.server.address'],
          path: '/api/vpc/list/' + result,
          port: self.config['accounts.server.port']
        };

        var request = https.request(options, function(response) {

          var data = '';
          response.on('data', function (chunk) {
            data += chunk;
          });

          response.on('end', function () {
            res.write(data);
            res.end();
          });

          response.on('error', function () {
            res.write('network');
            res.end();
          });
        });

        request.on('error', function () {
          res.write('network');
          res.end();
        });

        request.on('socket', function (socket) {
          socket.setTimeout(10000);
          socket.on('timeout', function() {

            request.abort();
            res.write('network');
            res.end();

          });
        });
        request.end();
      }
    });
  });

  /**
   * Save a credential / API Key.
   */
  this.app.post('/credential', function (req, res) {

    var apiKey = req.body.apiKey;

    // Write credentials to file.
    fs.writeFile(self.dirPath + '/.credential', apiKey,
      function (error, result) {
        if (error) {

          self.logger.warn('Could not write to ./.credential', error, result);
          res.write('Error: Could not write credentials file.');
          res.end();

        } else {
          self.Api.credential = apiKey;

          res.write('true');
          res.end();

        }
    });
  });


  /**
   * Healthcheck endpoint
   */
  this.app.get('/status', function (req, res) {
    res.send(200, 'OK');
  });


  /**
   * Catch port binding errors.
   */
  this.app.on('error', function () {
    self.logger.warn('Port ' + self.config['dashboard.bind.port'] + ' is in use.');
    process.exit(1);
  });
};

/**
 * Gets the local host.
 * @return {string} localhost ip address.
 */
WebAppServer.prototype.getLocalHost = function() {
  var os = require('os');
  var ifaces = os.networkInterfaces();
  var localhost = '';

  for (var dev in ifaces) {
    for (var i = 0, len = ifaces[dev].length; i < len; i++) {
      var details = ifaces[dev][i];
      if (details.family === 'IPv4') {
        if (dev === 'lo0') {
          localhost = details.address;
          break;
        }
      }
    }
  }
  return localhost;
};

/**
 * Export app.
 */
module.exports = WebAppServer;


