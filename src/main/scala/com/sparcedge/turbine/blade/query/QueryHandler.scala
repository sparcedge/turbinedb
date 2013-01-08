package com.sparcedge.turbine.blade.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import scala.concurrent.{ExecutionContext,Await}
import scala.concurrent.duration._
import akka.util.Timeout
import akka.pattern.ask
import spray.routing.RequestContext
import spray.http.{HttpResponse,HttpEntity,StatusCodes}
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
		case HandleQuery(query, eventCacheManager, ctx) =>
			val future = eventCacheManager ? EventCacheRequest()

			future onSuccess {
				case EventCacheResponse(eventCache) =>
					try {
						val results = eventCache.applyQuery(query)
						val json = "{\"results\":" + results + "}"
						// Complete Context With Result
						ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(json)))
					} catch {
						case e: Exception =>
							ctx.complete(HttpResponse(StatusCodes.InternalServerError))
					}

				case _ =>
			}
		case _ =>
	}
}

case class HandleQuery(query: TurbineQuery, cacheManager: ActorRef, ctx: RequestContext)

