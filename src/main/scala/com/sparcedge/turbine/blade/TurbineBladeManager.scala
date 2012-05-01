package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem}

class TurbineBladeManager extends Actor {
	
	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			def query = TurbineAnalyticsQuery.parse(rawQuery)
			println(query.createCacheSegmentString + ": " + query)
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)