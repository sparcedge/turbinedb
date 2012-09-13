package com.sparcedge.turbine.blade.cache

import akka.actor.{Actor,ActorRef}
import akka.dispatch.Future
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class EventCacheManager(implicit mongoConnection: MongoDBConnection) extends Actor {

	import context.dispatcher

	var eventCache: EventCache = null

	var unhandledRequests = List[(ActorRef,TurbineQuery)]()
	var cacheCheckouts = List[(UUID,Long)]()
	var eventCacheUpdateRequired = false
	var eventCacheUpdate: Option[EventUpdate] = None

	def receive = {
		case EventCacheRequest(query) =>
			if(eventCache == null) {
				eventCache = EventCache(query.blade)
			}
			requester ! EventCacheResponse(eventCache)
		case UpdateEventCacheWithNewEventsRequest() =>
			Future {
				eventCache.update()
			}
		case _ =>
	}

}

case class EventCacheRequest(query: TurbineQuery)

case class EventCacheResponse(eventCache: EventCache)

case class UpdateEventCacheWithNewEventsRequest()