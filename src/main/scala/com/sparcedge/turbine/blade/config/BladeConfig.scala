package com.sparcedge.turbine.blade.config

import net.liftweb.json._

object BladeConfig {

	implicit val formats = Serialization.formats(NoTypeHints)

	def apply(confJson: String): BladeConfig = {
		val jsonObj = parse(confJson)
		jsonObj.extract[BladeConfig]
	}
}

case class BladeConfig (
	mongo: MongoDB,
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
		"collection": "eventCollection" 
	}
}*/