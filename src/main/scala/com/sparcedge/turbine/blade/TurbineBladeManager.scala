package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import akka.routing.RoundRobinRouter
import akka.util.duration._
import java.util.concurrent.atomic.AtomicLong
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.query.{TurbineAnalyticsQuery,QueryHandler,HandleQuery}
import com.sparcedge.turbine.blade.query.cache.{EventCacheManager,UpdateEventCacheWithNewEventsRequest}
import scala.collection.mutable
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class TurbineBladeManager(mongoConnection: MongoDBConnection) extends Actor {
	
	val actorSegmentCacheMap = mutable.Map[String,ActorRef]()
	val queryHandlerRouter = context.actorOf(Props[QueryHandler].withRouter(RoundRobinRouter(50)), "QueryHandlerRouter")
	var eventCacheManagers = IndexedSeq[ActorRef]()
	val next = new AtomicLong(0)

	context.system.scheduler.schedule(
		30 seconds,
    	30 seconds,
    	self,
    	UpdateCurrentEventCaches()
    )

	context.system.scheduler.schedule(
		30 seconds,
    	30 seconds,
    	self,
    	UpdateOneHistoricalCache()
    )

	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			val query = TurbineAnalyticsQuery(rawQuery)
			val cacheKey = query.createCacheSegmentString()
			val cacheManager = actorSegmentCacheMap.getOrElseUpdate(cacheKey, {
				val man = context.actorOf(Props(new EventCacheManager(mongoConnection)), name = cacheKey)
				eventCacheManagers = eventCacheManagers :+ man
				man
			})
			queryHandlerRouter ! HandleQuery(query, cacheManager)
		case UpdateCurrentEventCaches() =>
			val dateSegment = new DateTime().toString("yyyy-MM")
			actorSegmentCacheMap.filter(_._1.endsWith(dateSegment)).foreach { case (segment, cacheManager) =>
				cacheManager ! UpdateEventCacheWithNewEventsRequest()
			}
		case UpdateOneHistoricalCache() =>
			if(!eventCacheManagers.isEmpty) {
				val man = eventCacheManagers((next.getAndIncrement % eventCacheManagers.size).asInstanceOf[Int])
				man ! UpdateEventCacheWithNewEventsRequest()
			}
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)
case class UpdateCurrentEventCaches()
case class UpdateOneHistoricalCache()