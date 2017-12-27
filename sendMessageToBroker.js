const request = require('request');
const amqp = require('amqplib/callback_api');

const MESSAGE_SERVER = process.env.RABBITMQ_BIGWIG_URL || 'amqp://localhost';
const QUEUE = 'metalAppQueue';
const URL = process.env.REF_DATA_SERVICE_URL || 'http://localhost:3030';

const streamMarket = () => {
    request({
        url: `${URL}/metals`,
        json: true
    }, (error, response, body) => {
        let message;
        if (error) {
            message = error;
        } else {
            message = body;
        }
        sendMessage(JSON.stringify(message), (err, data) => {
            if (err) {
                console.log('Error while sending message');
            } else {
                console.log(data);
            }
        });
    });
}

const sendMessage = (message, cb) => {
    amqp.connect(MESSAGE_SERVER, (err, conn) => {
        if (err) {
            console.log('sendMessageToBroker : Error while connecting to Messaging server');
            cb(err);
        } else if (conn) {
            conn.createChannel((error, ch) => {
                ch.assertQueue(QUEUE, { durable: false });
                ch.sendToQueue(QUEUE, new Buffer(message));
                cb(undefined, 'Message sent to broker : ' + message);
            });
            setTimeout(() => {
                conn.close();
            }, 500);
        }
    });
}

module.exports = { streamMarket, sendMessage };