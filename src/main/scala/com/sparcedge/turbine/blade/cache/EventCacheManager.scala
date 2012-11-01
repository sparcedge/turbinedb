package com.sparcedge.turbine.blade.cache

import akka.actor.{Actor,ActorRef}
import akka.dispatch.{Await,Future,Promise,ExecutionContext}
import akka.util.duration._
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.util.Timer

class EventCacheManager(blade: Blade)(implicit val mongoConnection: MongoDBConnection) extends Actor {

	import context.dispatcher

	val eventCacheFuture: Promise[EventCache] = Promise[EventCache]()
	var updateInProgress = false

	Future {
		updateInProgress = true
		val timer = new Timer
		timer.start()
		eventCacheFuture.complete(Right(EventCache(blade)))
		timer.stop("[EventCacheManager] Created Cache -- (" + blade + ")")
		updateInProgress = false
		updateEventCacheIfNotInProgress()
	}

	def receive = {
		case EventCacheRequest() =>
			val senderRef = sender
			eventCacheFuture onComplete {
				case Right(eventCache) => 
					senderRef ! EventCacheResponse(eventCache)
				case Left(failure) => // TODO Handle Exception
			}
		case UpdateEventCacheWithNewEventsRequest() =>
			updateEventCacheIfNotInProgress()
		case _ =>
	}

	private def updateEventCacheIfNotInProgress() {
		if(!updateInProgress) {
			updateInProgress = true
			Future {
				val timer = new Timer
				timer.start()
				await(eventCacheFuture).update()
				timer.stop("[EventCacheManager] Updated Cache (Blade: " + blade + ")")
				updateInProgress = false
			}
		}
	}

	private def await[T](future: Future[T]): T = {
		Await.result(future, 120 seconds)
	}
}

case class EventCacheRequest()

case class EventCacheResponse(eventCache: EventCache)

case class UpdateEventCacheWithNewEventsRequest()