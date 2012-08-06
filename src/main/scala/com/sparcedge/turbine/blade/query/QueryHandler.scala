package com.sparcedge.turbine.blade.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import akka.dispatch.ExecutionContext
import akka.util.duration._
import akka.util.Timeout
import com.mongodb.casbah.query.Imports._
import net.liftweb.json.JsonDSL._
import akka.pattern.ask

import net.liftweb.json._

import com.sparcedge.turbine.blade.query.cache._
import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class QueryHandler extends Actor {

	implicit val timeout = Timeout(120 seconds)
	implicit val formats = Serialization.formats(NoTypeHints)
	implicit val ec: ExecutionContext = context.dispatcher 

	def getStackTrace(ex: Exception): String = {
		val writer = new StringWriter()
		ex.printStackTrace(new PrintWriter(writer))
		writer.toString
	}

	def receive = {
		case HandleQuery(query, eventCacheManager) =>
			val future = eventCacheManager ? EventCacheRequest(query)

			future onSuccess {
				case EventCacheResponse(eventCache, id) =>
					try {
						val results = eventCache.applyQuery(query)
						val json = "{\"qid\":\"" + query.qid + "\",\"results\":" + results + "}"
						println(json)
					} catch {
						case e: Exception =>

							println("Exception Processing Query ID: " + query.qid + ", Error: " + getStackTrace(e))
					} finally {
						eventCacheManager ! EventCacheCheckin(id)
					}

				case _ =>
			}
		case _ =>
	}
}

case class Result (
	results: Iterable[String]
)

case class HandleQuery(query: TurbineAnalyticsQuery, cacheManager: ActorRef)

