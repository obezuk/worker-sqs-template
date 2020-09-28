import { AwsClient } from 'aws4fetch'

const aws = new AwsClient({
    "accessKeyId": AWS_ACCESS_KEY_ID,
    "secretAccessKey": AWS_SECRET_ACCESS_KEY,
    "region": AWS_DEFAULT_REGION
});

addEventListener('fetch', function(event) {
    event.respondWith(handleRequest(event))
});

addEventListener('scheduled', event => {
    event.waitUntil(handleQueue(event))
})

async function pollQueue(queueURL, waitTime) {
    var url = new URL(queueURL);
    url.searchParams.set('Action', 'ReceiveMessage');
    url.searchParams.set('MaxNumberOfMessages', 1);
    // url.searchParams.set('WaitTimeSeconds', 20); // On Schedule
    var queueReq = new Request(url, {
        "headers": {
            "Accept": "application/json"
        }
    });

    // TODO: Repeat this waitTime / 20 (3 times) until end of cron period.
    var jobs = await fetch(await aws.sign(queueReq));
    var body = await jobs.json();

    if (jobs.status !== 200) {
        console.log(jobs.status, body)
        throw new Error(JSON.stringify(body));
    }

    return body;
}

async function sendMessage(queueUrl, content) {
    console.log('sendMessage', queueUrl, content)
    var url = new URL(queueUrl);
    url.searchParams.set('Action', 'SendMessage');
    url.searchParams.set('MessageBody', content);
    var sendReq = new Request(url, {
        "headers": {
            "Accept": "application/json"
        }
    });
    var sendReq = await fetch(await aws.sign(sendReq));
}

async function deleteMessage(queueUrl, receiptHandle) {
    console.log('deleteMessage', queueUrl, receiptHandle)
    var url = new URL(queueUrl);
    url.searchParams.set('Action', 'DeleteMessage');
    url.searchParams.set('ReceiptHandle', receiptHandle);
    var deleteReq = new Request(url, {
        "headers": {
            "Accept": "application/json"
        }
    });
    var deleteMessage = await fetch(await aws.sign(deleteReq));
}

const AWS_QUEUE_URL = "https://sqs." + AWS_DEFAULT_REGION + ".amazonaws.com/" + AWS_ACCOUNT_ID + "/" + AWS_SQS_QUEUENAME;

async function compute(message) {
    console.log('compute', message); // do work here
}

async function handleMessage(queueUrl, message) {
    await compute(message);
    await deleteMessage(queueUrl, message.ReceiptHandle);
    await sendMessage(queueUrl, 'message'); // Send a dummy message to keep the queue running
    return true;
}

async function handleRequest(event) {
    return await handleQueue(event);
}

async function handleQueue(event) {
    var jobs = await pollQueue(AWS_QUEUE_URL, 60);
    if (jobs.ReceiveMessageResponse.ReceiveMessageResult.messages) {
        console.log('Received ' + jobs.ReceiveMessageResponse.ReceiveMessageResult.messages.length + ' job(s).');
        var handleMessages = [];
        for (var m in jobs.ReceiveMessageResponse.ReceiveMessageResult.messages) {
            handleMessages.push(handleMessage(AWS_QUEUE_URL, jobs.ReceiveMessageResponse.ReceiveMessageResult.messages[m]));
        }
        await Promise.all(handleMessages);
        return new Response('Completed Work', { status: 200 });
    } else {
        return new Response('No work in queue', { status: 200 });
    }
}