'use strict';

var dgram = require('dgram'),
    winston = require('winston'),
    mqtt = require('mqtt'),
    q = require('q'),
    program = require('commander'),
    pkg = require('./package.json');

var client  = mqtt.connect('mqtt://test.mosquitto.org');

function Server(config) {

    var mqttClient,
        udp,
        stats;

    function init() {

        stats = {
            startTime: new Date().getTime(),
            incomingMessages: 0,
            incomingBytes: 0,
            incomingReadings: 0,
            badMessages: 0
        };

        q.all([
            initialiseMQTTClient(config.mqttServer),
            initialiseSocket(config.listenPort)
        ]).then(function() {
            // Bind message events
            udp.on('message', onIncomingMessage);
        }).then(function() {
            // Start the pulse
            setInterval(function() {
                mqttClient.publish("system/udp-mqtt-relay/up", (new Date().getTime() - stats.startTime).toString());
                mqttClient.publish("system/udp-mqtt-relay/messages", stats.incomingMessages.toString());
                mqttClient.publish("system/udp-mqtt-relay/bytes", stats.incomingBytes.toString());
                mqttClient.publish("system/udp-mqtt-relay/badMessages", stats.badMessages.toString());
                mqttClient.publish("system/udp-mqtt-relay/readings", stats.incomingReadings.toString());

                // Reset the counters
                stats.incomingBytes = 0;
                stats.incomingMessages = 0;
                stats.incomingReadings = 0;
                stats.badMessages = 0;
            }, 1000);
            winston.info('Service is ready.');
        }).done();

    }

    function initialiseMQTTClient(serverAddress) {
        var deferred = q.defer();

        winston.info('Initialising MQTT connection...');
        mqttClient = mqtt.connect(serverAddress);
        mqttClient.on('connect', function() {
            winston.info('MQTT connection established with ' + serverAddress);
            deferred.resolve();
        });

        return deferred.promise;
    }

    /**
     * Initialise the UDP socket and start listening for data
     */
    function initialiseSocket(port) {
        winston.info('Initialising UDP socket...');

        var deferred = q.defer();

        udp = dgram.createSocket('udp4');
        udp.once('listening', function listening() {
            winston.info('UDP: Listening on port ' + port);
            deferred.resolve(udp.address());
        });
        udp.bind(port);

        return deferred.promise;
    }

    function onIncomingMessage(buffer, remoteInfo) {
        try {
            stats.incomingMessages += 1;
            var payload = buffer.toString();
            stats.incomingBytes += payload.length;
            var parts = payload.split(' ');
            var topic = parts.shift();
            stats.incomingReadings += parts.length;
            var data = parts.join(' ');
            mqttClient.publish(topic, data);
        } catch (e) {
            stats.badMessages += 1;
        }
    }

    init();
}

function run() {
    program
        .version(pkg.version)
        .option('-p, --listenPort [number]', 'the port to listen out for incoming connections.')
        .option('-m, --mqttServer [string]', 'the target mqtt server address.')
        .parse(process.argv);

    if (!program.listenPort) {
        throw new Error('Must specify a listen port.');
    }
    if (!program.mqttServer) {
        throw new Error('Must specify a target MQTT server address.');
    }

    new Server({
        mqttServer: program.mqttServer,
        listenPort: program.listenPort
    });
}

run();
