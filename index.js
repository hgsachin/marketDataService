const express = require('express');
const http = require('http');
const bodyParser = require('body-parser');

const brokerConnector = require('./sendMessageToBroker');

const messageService = express();
messageService.use(bodyParser.json());

messageService.post('/sendMessage', (req, res) => {
    var body = req.body;
    if (!body.message || body.message == '') {
        res.send('Error!! No message to be sent');
    } else {
        brokerConnector.sendMessage(body.message, (err, data) => {
            if (err) {
                res.send('Error while sending message');
            } else {
                res.send(data);
            }
        });
    }
});

const messageServer = http.createServer(messageService);
messageServer.listen(3010, () => {
    console.log('Message Server started on port 3010');
    setInterval(brokerConnector.streamMarket, 10000);
});