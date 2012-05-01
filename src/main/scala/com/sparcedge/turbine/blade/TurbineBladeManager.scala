package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem}
import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class TurbineBladeManager(mongoConnection: MongoDBConnection) extends Actor {
	
	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			def query = TurbineAnalyticsQuery(rawQuery)
			println(query.createCacheSegmentString + ": " + query)
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)