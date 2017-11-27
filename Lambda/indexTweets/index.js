urexports.handler = (event, context, callback) => {
  var AWS = require('aws-sdk');
  var elasticsearch=require('elasticsearch');
  var httpC=require('http-aws-es');
  AWS.config.update({
    accessKeyId: process.env.accessKeyId,
    secretAccessKey:process.env.secretAccessKey,
    region:process.env.region,
  });
  //create a elasticsearch client
  var client = new elasticsearch.Client({
    host: process.env.elasticsearchUrl,
    connectionClass:httpC,
  });

  var tweet = event.Records[0].Sns.Message;

  client.create({
    index: 'tweet',
    type: 'tweet',
    id:Math.floor(new Date()),
    body: tweet
  }, function (error, response) {
    if(error){
      console.error(error);
    } else {
      //console.log(response);
    }
  });
  callback(null, "Indexed");
}
