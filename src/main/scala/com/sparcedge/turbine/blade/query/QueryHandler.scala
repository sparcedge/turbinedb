package com.sparcedge.turbine.blade.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.Await
import akka.pattern.ask
import com.sparcedge.turbine.blade.cache._

class QueryHandler extends Actor {

	implicit val timeout = Timeout(240 seconds)
	implicit val ec: ExecutionContext = context.dispatcher 

	def getStackTrace(ex: Exception): String = {
		val writer = new StringWriter()
		ex.printStackTrace(new PrintWriter(writer))
		writer.toString
	}

	def receive = {
		case HandleQuery(query, eventCacheManager) =>
			val future = eventCacheManager ? EventCacheRequest()

			future onSuccess {
				case EventCacheResponse(eventCache) =>
					try {
						val results = eventCache.applyQuery(query)
						val json = "{\"qid\":\"" + query.qid + "\",\"results\":" + results + "}"
						println(json)
					} catch {
						case e: Exception =>
							println("Exception Processing Query ID: " + query.qid + ", Error: " + getStackTrace(e))
					}

				case _ =>
			}
		case _ =>
	}
}

case class HandleQuery(query: TurbineQuery, cacheManager: ActorRef)

