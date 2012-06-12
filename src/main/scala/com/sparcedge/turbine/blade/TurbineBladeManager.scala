package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import akka.routing.RoundRobinRouter
import akka.util.duration._
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.query.{TurbineAnalyticsQuery,QueryHandler,HandleQuery,EventCacheManager,UpdateEventCacheWithNewEventsRequest}
import scala.collection.mutable
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class TurbineBladeManager(mongoConnection: MongoDBConnection) extends Actor {
	
	val actorSegmentCacheMap = mutable.Map[String,ActorRef]()
	val queryHandlerRouter = context.actorOf(Props[QueryHandler].withRouter(RoundRobinRouter(50)), "QueryHandlerRouter")

	context.system.scheduler.schedule(
		10 seconds,
    	10 seconds,
    	self,
    	UpdateCurrentEventCaches()
    )

	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			val query = TurbineAnalyticsQuery(rawQuery)
			val cacheKey = query.createCacheSegmentString()
			val cacheManager = actorSegmentCacheMap.getOrElseUpdate(cacheKey, {
				context.actorOf(Props(new EventCacheManager(mongoConnection)), name = cacheKey)
			})
			queryHandlerRouter ! HandleQuery(query, cacheManager)
		case UpdateCurrentEventCaches() =>
			val dateSegment = new DateTime().toString("yyyy-MM")
			actorSegmentCacheMap.filter(_._1.endsWith(dateSegment)).foreach { case (segment, cacheManager) =>
				cacheManager ! UpdateEventCacheWithNewEventsRequest()
			}
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)
case class UpdateCurrentEventCaches()