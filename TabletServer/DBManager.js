function  DBManager(){

	this.MongoClient = null
	this.connection = null;
	this.db = null;
}
	DBManager.prototype.connectionSucceed = function(err, db)
	{

		if (err) throw err;
		  this.db = db.db("tabletserver");
		  console.log("MONGO CONNECTED");
	}

	DBManager.prototype.connect = function()
	{

		this.MongoClient = require('mongodb').MongoClient;
		this.MongoClient.connect("mongodb://127.0.0.1:27017/",this.connectionSucceed);

	}

	DBManager.prototype.update = function(query, new_values, collection)
	{
		
		if(this.db == null)
		{
			console.log("null");
			return;
		}

		console.log(collection);
		this.db.collection("dns").updateOne(query, new_values, function(err, res) {
		    if (err) throw err;
		    this.dbo.close();
	  	});
	}



module.exports = DBManager;