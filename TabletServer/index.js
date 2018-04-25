var express = require('express')
var app = express()
var dbo;
var DBManager = require('./DBManager.js')
app.get('/', function (req, res) {
  res.send('Hello World')
})
app.get('')
 
app.listen(3000)
var dbm = new DBManager();
dbm.connect();

var myquery = { domain_name: "twitter.com" };
newvalues = { $set: {domain_name: "facebook.com"} };
Sleep(300);
dbm.update(myquery, newvalues, "dns");
// var MongoClient = require('mongodb').MongoClient;
// var url = "mongodb://127.0.0.1:27017/";

// MongoClient.connect(url, function(err, db) {
//   if (err) throw err;
//   db = db.db("tabletserver");
//   console.log("MONGO CONNECTED");
//   var myquery = { domain_name: "facebook.com" };
//   var newvalues = { $set: {domain_name: "twitter.com"} };
//   dbo.collection("dns").updateOne(myquery, newvalues, function(err, res) {
//     if (err) throw err;
//     console.log("1 document updated");
//     db.close();
//   });
//}); 