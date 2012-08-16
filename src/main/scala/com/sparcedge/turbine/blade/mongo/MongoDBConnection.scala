package com.sparcedge.turbine.blade.mongo

import com.sparcedge.turbine.blade.config.{BladeConfig,MongoDBServer}
import com.mongodb.casbah.{MongoConnection,MongoCollection}
import com.mongodb.ServerAddress

object MongoDBConnection {
	def apply(config: BladeConfig): MongoDBConnection = {
		new MongoDBConnection(config.mongo.servers, config.mongo.database, config.mongo.collection, config.mongo.batchSize.getOrElse(5000))
	}
}

class MongoDBConnection(servers: List[MongoDBServer], database: String, collection: String, val batchSize: Int) {

	val serverAddressList = servers map { s => new ServerAddress(s.host, s.port) }
	val mongoConnection = serverAddressList match {
		case server :: Nil =>
			MongoConnection(server)
		case servers =>
			MongoConnection(servers)
	}

	def collection: MongoCollection = {
		mongoConnection(database)(collection)
	}
}