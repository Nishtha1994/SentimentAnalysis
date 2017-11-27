
exports.handler = (event, context, callback) => {
  var AWS = require('aws-sdk');
  var elasticsearch=require('elasticsearch');
  var httpes=require('http-aws-es');
  AWS.config.update({
    accessKeyId: process.env.accessKeyId,
    secretAccessKey:process.env.secretAccessKey,
    region:process.env.region,
  });
  //create a elasticsearch client
  var client = new elasticsearch.Client({
    host: process.env.elasticsearch,
    connectionClass:httpes,
  });

  var allRecords = [];
  var date = new Date();
  date.setMinutes(date.getMinutes()-3600);

  client.search({
    index: 'tweet',
    type: 'tweet',
    scroll: '10s',
    body: {
      query: {
        match: {
          content: event.track
        }
      }
    }
  },function getMoreUntilDone(error, response) {
    if(error){
      console.log(error);
      callback(error,null);
    } else {
      response.hits.hits.forEach(function (hit) {
        allRecords.push(hit);
      });
      if (response.hits.total !== allRecords.length) {
        client.scroll({
          scrollId: response._scroll_id,
          scroll: '10s'
        }, getMoreUntilDone);
      } else {
        var arr=[];
        for (var i = 0, len = allRecords.length; i < len; i++) {
          arr.push(allRecords[i]._source);
        }
        callback(null,{tweets:arr});
      }
    }
  });
}
