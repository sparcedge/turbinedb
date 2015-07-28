
# Turbine DB

Scala code base for the turbine database, which is a low latency, time-series, event driven, columnar database.

## Applications Powered by TurbineDB

* [Any Viz](https://github.com/sparcedge/any-viz) - A simple, query builder for TurbineDB written in Clojure. 
* [Data Loader](https://github.com/sparcedge/turbine-data-loader) - A project to help load data into TurbineDB.
* [Hack Viz](https://github.com/gilbertw1/hackviz) - A GitHub commit visualizer using TurbineDB.
* [OSX Usage Stats](https://github.com/bobwilliams/osx-usage-stats) - MacOS X memory and application usage stats powered by TurbineDB and written in Python.
* [VMStats TurbineDB](https://github.com/bobwilliams/vmstats-turbinedb) - A Scala app to logs data from the vm_stats command into TurbineDB.

If you would like to showcase your usage of TurbineDB please submit a pull request for inclusion.

## Prerequisites

* [SBT](https://github.com/harrah/xsbt) -- [install instructions](http://www.scala-sbt.org/release/docs/Getting-Started/Setup)
* [Scala IDE (Eclipse)](http://www.scala-ide.org/) -- [install](http://download.scala-ide.org/)

## Getting Started

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

## Basic Use

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

## License

This project is Copyright (c) 2015 [SPARC](https://github.com/sparcedge/) and open sourced under the [GNU GPL v3.0](LICENSE.txt).