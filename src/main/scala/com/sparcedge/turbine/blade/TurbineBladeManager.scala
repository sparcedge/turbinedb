package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem}

class TurbineBladeManager extends Actor {
	
	def receive = {
		case QueryDispatchRequest(rawQuery) =>
			println(rawQuery)
		case _ =>
	}

}

case class QueryDispatchRequest(rawQuery: String)