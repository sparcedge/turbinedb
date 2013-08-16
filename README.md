
Turbine DB
=============

Scala code base for the turbine database, which is a low latency time series event driven, columnar database.

Prerequisites
------------

* [SBT](https://github.com/harrah/xsbt) -- [install instructions](http://www.scala-sbt.org/release/docs/Getting-Started/Setup)
* [Scala IDE (Eclipse)](http://www.scala-ide.org/) -- [install](http://download.scala-ide.org/)

Getting Started
------------

Download Project References
	
	$ sbt update

Generate Eclipse Project

	$ sbt eclipse

Compile

	$ sbt compile

Test	

	$ sbt test

Run

	$ sbt run

Package (Create Jar)

	$ sbt one-jar

Run Benchmarks (Google Caliper)

	$ sbt benchmark/run

Basic Use
---------

### Insert Event


	POST: http://localhost:8080/db/mydatabase/mycollection 

	{
		"timestamp": <timestamp>,
		"data": {
			"cpu": 43.2,
			"ram": 56.9
		}	
	}


### Queries
Only a reducer is required.

	GET: http://localhost:8080/db/mydatabase/mycollection?q=<query>

	{
		"start": <timestamp>, // Optional
		"end": <timestamp>, // Optional
		"extend": [
			{"ext-value": ["add", "segment1", ["mul", "segment2", "segment3"]]}
		], "match": [ // Optional
			{"ram": {"gt": 50}}
		], "group": [ // Optional
			{"duration": "hour"}
		], "reduce": [ // Required
			{"ram-avg": {"avg": "ram"}}
		]
	}


### Notifications
Query needs to be URI encoded [eg: encodeURIComponent(query) in node/js console]

	GET: http://localhost:8080/db/mydatabase/mycollection?m=<query>


