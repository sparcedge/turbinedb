package com.sparcedge.turbine.blade.cache

import akka.actor.{Actor,ActorRef}
import akka.dispatch.Future
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class EventCacheManager(blade: Blade)(implicit val mongoConnection: MongoDBConnection) extends Actor {

	import context.dispatcher

	var eventCache: EventCache = null
	// TODO: Handle Queries that may come in before this is ready
	Future {
		eventCache = EventCache(blade)
	}

	def receive = {
		case EventCacheRequest(query) =>
			sender ! EventCacheResponse(eventCache)
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