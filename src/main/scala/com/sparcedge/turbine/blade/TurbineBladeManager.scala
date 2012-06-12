package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import akka.routing.RoundRobinRouter
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.query.{TurbineAnalyticsQuery,QueryHandler,HandleQuery,EventCacheManager}
import scala.collection.mutable

class TurbineBladeManager(mongoConnection: MongoDBConnection) extends Actor {
	
	val actorSegmentCacheMap = mutable.Map[String,ActorRef]()
	val queryHandlerRouter = context.actorOf(Props[QueryHandler].withRouter(RoundRobinRouter(50)), "QueryHandlerRouter")

	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			val query = TurbineAnalyticsQuery(rawQuery)
			val cacheKey = query.createCacheSegmentString()
			val cacheManager = actorSegmentCacheMap.getOrElseUpdate(cacheKey, {
				context.actorOf(Props(new EventCacheManager(mongoConnection)), name = cacheKey)
			})
			queryHandlerRouter ! HandleQuery(query, cacheManager)
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)