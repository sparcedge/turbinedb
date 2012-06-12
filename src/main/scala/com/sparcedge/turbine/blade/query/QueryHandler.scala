package com.sparcedge.turbine.blade.query

import akka.actor.{Actor,ActorRef}
import akka.util.duration._
import akka.util.Timeout
import com.mongodb.casbah.query.Imports._
import net.liftweb.json.JsonDSL._
import akka.pattern.ask

import net.liftweb.json._

import com.sparcedge.turbine.blade.mongo.MongoDBConnection

class QueryHandler extends Actor {

	implicit val timeout = Timeout(60 seconds)
	implicit val formats = Serialization.formats(NoTypeHints)

	def receive = {
		case HandleQuery(query, eventCacheManager) =>
			val future = eventCacheManager ? EventCacheRequest(query)

			future onSuccess {
				case EventCacheResponse(eventCache, id) =>
					try {
						val results = eventCache.applyQuery(query)
						val json = Map[String,Any](
							"results" -> results, 
							"qid" -> query.qid
						)
						println(Serialization.write(json))
					} catch {
						case e: Exception =>
							println("Exception Processing Query ID: " + query.qid + ", Error: " + e.getStackTrace)
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

