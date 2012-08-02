package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import akka.routing.RoundRobinRouter
import akka.util.duration._
import java.util.concurrent.atomic.AtomicLong
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.query.{TurbineAnalyticsQuery,QueryHandler,HandleQuery}
import com.sparcedge.turbine.blade.query.cache.{EventCacheManager,UpdateEventCacheWithNewEventsRequest}
import scala.collection.mutable
import scala.io.Source
import java.io.File
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
			var newQuery = rawQuery
			if(rawQuery == "runit") {
				val source = Source.fromFile(new File("/Users/bgilbert/Desktop/810-turbine-query-raw.json"))
				val lines = source.getLines.toList
				newQuery = lines.head
				lines.tail.foreach { line =>
					self ! QueryDispatchRequest(line)
				}
				//newQuery = "{\"blade\":{\"domain\":\"4f6a2eec61faa7fe1d000035\",\"tenant\":\"4f6a314d61faa7fe1d000051\",\"category\":\"clamp-data\",\"period\":\"2012-08\"},\"query\":{\"range\":{\"start\":1343433600000,\"end\":1343853715041},\"category\":\"clamp-data\",\"match\":{\"resource\":{\"eq\":\"810vermont\"}},\"group\":[{\"type\":\"duration\",\"value\":\"day\"}],\"reduce\":{\"reducers\":[{\"propertyName\":\"bldg-coolingtower-1a-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-1a-kW\"},{\"propertyName\":\"bldg-coolingtower-1b-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-1b-kW\"},{\"propertyName\":\"bldg-coolingtower-2a-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-2a-kW\"},{\"propertyName\":\"bldg-coolingtower-2b-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-2b-kW\"},{\"propertyName\":\"bldg-coolingtower-3a-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-3a-kW\"},{\"propertyName\":\"bldg-coolingtower-3b-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-3b-kW\"},{\"propertyName\":\"bldg-coolingtower-4a-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-4a-kW\"},{\"propertyName\":\"bldg-coolingtower-4b-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-coolingtower-4b-kW\"},{\"propertyName\":\"bldg-cthtrs-hpb-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-cthtrs-hpb-kW\"},{\"propertyName\":\"bldg-pump-1-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-1-kW\"},{\"propertyName\":\"bldg-pump-10-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-10-kW\"},{\"propertyName\":\"bldg-pump-11-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-11-kW\"},{\"propertyName\":\"bldg-pump-12-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-12-kW\"},{\"propertyName\":\"bldg-pump-13-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-13-kW\"},{\"propertyName\":\"bldg-pump-14-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-14-kW\"},{\"propertyName\":\"bldg-pump-15-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-15-kW\"},{\"propertyName\":\"bldg-pump-16-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-16-kW\"},{\"propertyName\":\"bldg-pump-17-kW\",\"reducer\":\"avg\",\"segment\":\"bldg-pump-17-kW\"}]}},\"qid\":\"q1\"}"
			}
			val query = TurbineAnalyticsQuery(newQuery)
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