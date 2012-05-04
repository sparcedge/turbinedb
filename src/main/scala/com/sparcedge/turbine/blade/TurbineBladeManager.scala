package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.query.{TurbineAnalyticsQuery,QueryHandler,HandleQuery}
import scala.collection.mutable

class TurbineBladeManager(mongoConnection: MongoDBConnection) extends Actor {
	
	val actorSegmentCacheMap = mutable.Map[String,ActorRef]()

	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			val query = TurbineAnalyticsQuery(rawQuery)
			val cacheKey = query.createCacheSegmentString()
			val handler = actorSegmentCacheMap.getOrElseUpdate(cacheKey, {
				context.actorOf(Props(new QueryHandler(mongoConnection)), name = cacheKey)
			})
			handler ! HandleQuery(query)
			//println(query.createCacheSegmentString + ": " + query)
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)