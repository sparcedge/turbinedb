package com.sparcedge.turbine.blade.query

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.Imports._

import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class QueryHandler(mongoConnection: MongoDBConnection) extends Actor {

	var eventCache: EventCache = null

	def populateEventCache(query: TurbineAnalyticsQuery) = {
		val collection = mongoConnection.collection
		val q: MongoDBObject = 
			("ts" $gte query.blade.periodStart.getMillis $lt query.blade.periodEnd.getMillis) ++ 
			("d" -> new ObjectId(query.blade.domain)) ++
			("t" -> new ObjectId(query.blade.tenant)) ++
			("c" -> query.blade.category) 
		val fields = query.query.requiredFields.map(("dat." + _ -> 1)).foldLeft(MongoDBObject())(_ ++ _) ++ ("r" -> 1) ++ ("ts" -> 1)
		val cursor = collection.find(q, fields)
		cursor.batchSize(5000)
		eventCache = EventCache(cursor)
		cursor.close()
	}

	def receive = {
		case HandleQuery(query) =>
			if(eventCache == null) {
				populateEventCache(query)
			}
			println("EventCache: " + eventCache)
			val events = eventCache.applyQuery(query)
			println(events.size)
		case _ =>
	}
}

case class HandleQuery(query: TurbineAnalyticsQuery)