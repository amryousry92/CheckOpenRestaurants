var express = require('express')
// , stylus = require('stylus')
// , nib = require('nib')
var app = express()
// function compile(str, path) {
//   return stylus(str)
//     .set('filename', path)
//     .use(nib())
// }
// app.set('views', __dirname + '/views')
// app.set('view engine', 'jade')
// app.use(express.logger('dev'))
// app.use(stylus.middleware(
//   { src: __dirname + '/public'
//   , compile: compile
//   }
// ))
// app.use(express.static(__dirname + '/public'))

var fs    = require("fs")
var elasticsearch = require('elasticsearch');
var parse = require('csv-parse');
var async = require('async');
global.data = [];

var inputFile='restaurants.csv';

var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

//bulk index elasticsearch
const bulkIndex = function(index, type, input) {
  bulkBody = [];
  input.forEach(item => {
    bulkBody.push({
      index: {
        _index: index,
        _type: type,
        _id: item.id
      }
    });

    bulkBody.push(item);
  });

  client.bulk({body: bulkBody})
  .then(response => {
    console.log('here');
    errorCount = 0;
    response.items.forEach(item => {
      if (item.index && item.index.error) {
        console.log(++errorCount, item.index.error);
      }
    });
    console.log(
      `Successfully indexed ${input.length - errorCount}
      out of ${input.length} items`
    );
  })
  .catch(console.err);
};

// get hours from time
var timeFormat=function(timeString){
  timeSplit=timeString.split(" ");
  state = timeSplit[1];
  time=timeSplit[0];
  hours=parseInt(timeSplit[0].split(":")[0]);
  if((state=="PM" && hours!=12) || (state=="AM" && hours==12)){
    hours=(hours+12)%24;
  }
  return hours;
}

// Construct bitmap of open hours
var generateBitMap=function(hourOpen,hourClose){
  i=0;
  bitFlag=false;
  while(i<24){
    if(hourClose<=i)
    bitFlag=false;
    if((hourClose<12 && hourClose>=i && hourOpen>hourClose) || (hourOpen<=i) || (hourClose==hourOpen))
      bitFlag=true;
    if(hourOpen<hourClose && i>=hourClose)
      bitFlag=false;

    if(bitFlag)
      bitmap=bitmap+"1";
    else
      bitmap=bitmap+"0"

    i=i+1;
  }
  return bitmap;
}

// generate regex to be used in search query
var getRegex=function(){
  date = new Date();
  current_hour = date.getHours();
  i=0;
  regex="";
  while(i<24){
    if(i!=current_hour)
      regex=regex+"[0-1]";
    else
      regex=regex+1;

    i+=1;
  }
  return regex;
}
var parser = parse({delimiter: ','}, function (err, input) {
  async.eachSeries(input, function (jsonElement, callback) {

    bitmap="";
    if(jsonElement[0]!="id")
    {
      hourOpen=timeFormat(jsonElement[4]);
      hourClose=timeFormat(jsonElement[5]);
      bitmap=generateBitMap(hourOpen,hourClose);
      global.data.push({id: jsonElement[0],name: jsonElement[13],bitmap: bitmap})
    }
    callback();

  })
})

// file is read here and data added to elasticsearch
app.get('/readFile', function (req, res, next) {
  fs.createReadStream(inputFile).pipe(parser);
  // console.log(global.data);
  bulkIndex('restaurants', 'restaurant', global.data);
});


// Import file into elasticsearch
app.get('/getOpen', function (req, res, next) {
  regex=getRegex();
  client.search({
  index: 'restaurants',
  type: 'restaurant',
  body: {
    query: {
      regexp: {
        bitmap: regex
      }
    }
  }
  }).then(function (resp) {
       hits = resp.hits.hits;
       res.send(hits);
      //  for(var i=0;i<hits.size();i++){

        //  res.send(hits[i]["_source"]["name"]);
      //  }

  }, function (err) {
      console.trace(err.message);
  });
})

app.listen(3000, function () {
  console.log('Listening on port 3000!')
})
