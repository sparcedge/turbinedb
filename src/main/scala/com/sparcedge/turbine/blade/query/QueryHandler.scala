package com.sparcedge.turbine.blade.query

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.Imports._

import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class QueryHandler(mongoConnection: MongoDBConnection) extends Actor {

	def receive = {
		case HandleQuery(query) =>
			val collection = mongoConnection.collection
			val range = query.query.range.end match {
				case Some(end) =>
					("ts" $gte query.query.range.start $lt query.query.range.end.get)
				case None =>
					("ts" $gte query.query.range.start)
			}
			val q: MongoDBObject = range ++ 
				("d" -> new ObjectId(query.blade.domain)) ++
				("t" -> new ObjectId(query.blade.tenant)) ++
				("c" -> query.blade.category)

			println(collection.find(q).count)
		case _ =>
	}
}

case class HandleQuery(query: TurbineAnalyticsQuery)