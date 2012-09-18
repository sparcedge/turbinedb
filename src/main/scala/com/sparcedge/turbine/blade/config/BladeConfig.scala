package com.sparcedge.turbine.blade.config

import net.liftweb.json._
import com.sparcedge.turbine.blade.query.Blade

object BladeConfig {

	implicit val formats = Serialization.formats(NoTypeHints)

	def apply(confJson: String): BladeConfig = {
		val jsonObj = parse(confJson)
		jsonObj.extract[BladeConfig]
	}
}

case class BladeConfig (
	mongo: MongoDB,
	preloadBlades: Option[List[Blade]] = Some(List[Blade]()),
	dataDirectory: Option[String] = Some("data"),
	printTimings: Option[Boolean] = Some(false)
)

case class MongoDB (
	servers: List[MongoDBServer],
	database: String,
	collection: String,
	batchSize: Option[Int] = Some(5000)
)

case class MongoDBServer (
	host: String,
	port: Int
)

/*{
	"mongo": {
		"servers": [{
			"host": "127.0.0.1",
			"port": 12345
		}],
		"database": "events",
		"collection": "eventCollection",
		"batchSize": 10000
	},
	"dataDirectory": "data",
	"preloadBlades": [
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"}
	]
	"printTimings": true
}*/