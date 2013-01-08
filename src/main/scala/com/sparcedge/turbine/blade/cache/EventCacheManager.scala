package com.sparcedge.turbine.blade.cache

import akka.actor.{Actor,ActorRef}
import scala.util.{Try, Success, Failure}
import scala.concurrent.{Await,Future,Promise,ExecutionContext}
import scala.concurrent.duration._
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.util.Timer

class EventCacheManager(blade: Blade) extends Actor {

	import context.dispatcher

	val eventCachePromise = Promise[EventCache]() 
	val eventCacheFuture = eventCachePromise.future

	Future {
		val timer = new Timer
		timer.start()
		val eventCache = EventCache(blade)
		eventCachePromise.success(eventCache)
		timer.stop("[EventCacheManager] Created Cache -- (" + blade + ")")
	}

	def receive = {
		case EventCacheRequest() =>
			val senderRef = sender
			eventCacheFuture onComplete {
				case Success(eventCache) => 
					senderRef ! EventCacheResponse(eventCache)
				case Failure(error) => // TODO Handle Exception
			}
		case UpdateEventCacheWithNewEventsRequest() =>
			// TODO: Remove all update logic
		case _ =>
	}

	private def await[T](future: Future[T]): T = {
		Await.result(future, 120 seconds)
	}
}

case class EventCacheRequest()

case class EventCacheResponse(eventCache: EventCache)

case class UpdateEventCacheWithNewEventsRequest()