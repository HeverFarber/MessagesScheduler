const express = require('express');
const bodyParser = require('body-parser');
const uuidv4 = require('uuid/v4');

const redis = require("redis");
const client = redis.createClient();

const {promisify} = require('util');
const zadd = promisify(client.zadd).bind(client);
const zrange = promisify(client.zrange).bind(client);
const zrem = promisify(client.zrem).bind(client);

const MESSAGES_QUEUE = "messages";

const app = express();
app.use(bodyParser.json());

app.post('/echoAtTime', async function(req, res) {
    let timestamp = new Date(req.body.time).getTime();
    let message = JSON.stringify({timestamp, date: req.body.time ,message: req.body.message, guid: uuidv4()});
    await zadd(MESSAGES_QUEUE, timestamp, message);
    res.status(202).end();
});

app.listen(8088, () => {
    console.log("App listening at 8088");
});

async function loop() {
    while (true) {
        if (await isManager()) {
            try {
                const msg = await tryPullMessage();

                if (msg) {
                    processMsg(msg);
                } else {
                    await wait(1000);
                }
            } catch (e) {
                console.error(e);
                await wait(10000);
            }
        } else {
            await wait(60000);
        }
    }
}

async function isManager() {
    return true;
}

async function wait(timeout) {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve()
        }, timeout)
    })
}

async function tryPullMessage() {
    const result = await zrange(MESSAGES_QUEUE, 0, 0);
    const msg = result.length == 1 ? JSON.parse(result[0]) : null;
    
    if (msg && msg.timestamp < new Date().getTime()) {
        const count = await zrem(MESSAGES_QUEUE, result);
        return count === 1 ? msg : null;
    }
    
    return;
}

async function processMsg(msg) {
    console.log(msg.date + " : " + msg.message);
}

async function starter() {
    await loop();
}

starter();