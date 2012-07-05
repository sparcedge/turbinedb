package com.sparcedge.turbine.blade.query

import akka.actor.{Actor,ActorRef}
import akka.dispatch.Future
import com.mongodb.casbah.query.Imports._
import net.liftweb.json.JsonDSL._
import scala.collection.mutable
import java.util.UUID

import net.liftweb.json._

import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class EventCacheManager(mongoConnection: MongoDBConnection) extends Actor {

	import context.dispatcher

	var eventCache: EventCache = null

	var unhandledRequests = List[(ActorRef,TurbineAnalyticsQuery)]()
	var cacheCheckouts = List[(UUID,Long)]()
	var eventCacheUpdateRequired = false
	var eventCacheUpdate: Option[EventUpdate] = None

	def populateEventCache(query: TurbineAnalyticsQuery) {
		populateEventCache(query.blade, query.query.requiredFields)
	}

	def populateEventCache(blade: Blade, requiredFields: Set[String]) {
		val collection = mongoConnection.collection
		val q: MongoDBObject = 
			("ts" $gte blade.periodStart.getMillis $lt blade.periodEnd.getMillis) ++ 
			("d" -> new ObjectId(blade.domain)) ++
			("t" -> new ObjectId(blade.tenant)) ++
			("c" -> blade.category) 
		val fields = requiredFields.map(("dat." + _ -> 1)).foldLeft(MongoDBObject())(_ ++ _) ++ ("r" -> 1) ++ ("ts" -> 1) ++ ("its" -> 1)
		val cursor = collection.find(q, fields)
		cursor.batchSize(5000)
		eventCache = EventCache(cursor, blade.periodStart.getMillis, blade.periodEnd.getMillis, requiredFields, blade)
		cursor.close()
	}

	def updateEventCache(query: TurbineAnalyticsQuery) {
		updateEventCache(query.blade, query.query.requiredFields)
	}

	def updateEventCache(queries: List[TurbineAnalyticsQuery]) {
		val requiredFields = queries.flatMap(_.query.requiredFields)
		updateEventCache(queries.head.blade, requiredFields.toSet)
	}

	def updateEventCache(blade: Blade, fields: Set[String]) {
		val includedFields = eventCache.includedFields
		eventCache = null
		populateEventCache(blade, fields ++ includedFields)
	}

	def eventCacheHasAllRequiredData(query: TurbineAnalyticsQuery): Boolean = {
		eventCache.includesAllFields(query.query.requiredFields)
	}

	def checkoutCacheToRequester(requester: ActorRef) {
		val id = UUID.randomUUID
		cacheCheckouts = cacheCheckouts :+ (id -> System.currentTimeMillis)
		requester ! EventCacheResponse(eventCache, id)
	}

	def applyEventUpdateToCache(eventUpdate: EventUpdate) {
		println("Starting Update")
		val startMillis = System.currentTimeMillis
		eventCache.applyEventUpdate(eventUpdate)
		eventCacheUpdateRequired = false
		val endMillis = System.currentTimeMillis
		println("Finished update (" + (endMillis - startMillis) + "ms)")
	}

	def retrieveCurrentEventUpdate(): EventUpdate = {
		val startMillis = System.currentTimeMillis
		val blade = eventCache.blade
		val collection = mongoConnection.collection
		val q: MongoDBObject = 
			("ts" $gt blade.periodStart.getMillis $lt blade.periodEnd.getMillis) ++ 
			("its" $gt eventCache.newestTimestamp) ++
			("d" -> new ObjectId(blade.domain)) ++
			("t" -> new ObjectId(blade.tenant)) ++
			("c" -> blade.category)
		val fields = eventCache.includedFields.map(("dat." + _ -> 1)).foldLeft(MongoDBObject())(_ ++ _) ++ ("r" -> 1) ++ ("ts" -> 1) ++ ("its" -> 1)
		val cursor = collection.find(q, fields)
		cursor.batchSize(5000)
		val eventUpdate = EventCache.convertCursorToEventUpdate(cursor)
		cursor.close()
		eventUpdate
	}

	def prepareAndScheduleEventUpdate() {
		Future {
			val currentUpdate = retrieveCurrentEventUpdate()
			self ! ApplyEventUpdatetoCacheRequest(currentUpdate)
		} onFailure {
			case _ => prepareAndScheduleEventUpdate()
		}
	}

	def receive = {
		case EventCacheRequest(query) =>
			if(eventCache == null) {
				populateEventCache(query)
				checkoutCacheToRequester(sender)
			} else if(eventCacheHasAllRequiredData(query)) {
				checkoutCacheToRequester(sender)
			} else if(cacheCheckouts.size <= 0) {
				updateEventCache(query)
				checkoutCacheToRequester(sender)
			} else {
				unhandledRequests = unhandledRequests :+ (sender -> query)
			}
		case EventCacheCheckin(id) =>
			cacheCheckouts = cacheCheckouts.filterNot(_._1 == id)
			if(cacheCheckouts.size <= 0 && unhandledRequests.size > 0) {
				updateEventCache(unhandledRequests.map(_._2))
				unhandledRequests.foreach(ur => checkoutCacheToRequester(ur._1))
				unhandledRequests = List[(ActorRef,TurbineAnalyticsQuery)]()
			}
			if(cacheCheckouts.size <= 0) {
				eventCacheUpdate.foreach(applyEventUpdateToCache(_))
				eventCacheUpdate = None
			}
		case UpdateEventCacheWithNewEventsRequest() =>
			if(!eventCacheUpdateRequired) {
				eventCacheUpdateRequired = true
				prepareAndScheduleEventUpdate()
			}
		case ApplyEventUpdatetoCacheRequest(eventUpdate) =>
			if(cacheCheckouts.size <= 0) {
				applyEventUpdateToCache(eventUpdate)
			} else {
				eventCacheUpdate = Some(eventUpdate)
			}
		case _ =>
	}

}

case class EventCacheRequest(query: TurbineAnalyticsQuery)

case class EventCacheResponse(eventCache: EventCache, id: UUID)

case class EventCacheCheckin(id: UUID)

case class UpdateEventCacheWithNewEventsRequest()

case class ApplyEventUpdatetoCacheRequest(eventUpdate: EventUpdate)