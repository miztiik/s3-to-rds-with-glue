'use strict';
const AWS = require('aws-sdk');
const s3 = new AWS.S3();



exports.handler = async (event, context, callback) => {
  console.log(`BEGIN:${new Date()}`);

  let bktName = process.env.STORE_EVENTS_BKT;
  let bktPrefix = "store_events/node/";
  let objName = '101.json'; 
  let objData = '{ "message" : "H World!" }'; 
  const objType = 'application/json'; 
  try {

    // setup params for putObject
    const params = {
       Bucket: bktName,
       Key: objName,
       Body: objData,
       ContentType: objType,
    };
    const resp = await s3.putObject(params).promise();
    console.info("Status\n" + JSON.stringify(resp, null, 2));
    console.log(`File uploaded success`);
    console.log(context.logStreamName);
  } catch (error) {
    console.warn('error');
    //console.log(`Status: ${response.status}`)

  }
  return {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Upload Success!',
      input: event
    }),
  };
};