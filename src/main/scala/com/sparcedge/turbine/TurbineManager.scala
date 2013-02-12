package com.sparcedge.turbine

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import akka.routing.RoundRobinRouter
import spray.routing.RequestContext

import com.sparcedge.turbine.data.BladeManager
import com.sparcedge.turbine.query.{TurbineQueryPackage,QueryHandler,Blade}

object TurbineManager {

	case class QueryDispatchRequest(rawQuery: String, ctx: RequestContext)
}

import TurbineManager._
import QueryHandler._
import BladeManager._

class TurbineManager(preloadBlades: List[Blade]) extends Actor {

	val bladeRepositoryManager = context.actorOf(Props(new BladeManagerRepository(preloadBlades)), "BladeRepositoryManager")
	val queryHandlerRouter = context.actorOf (
		Props(new QueryHandler(bladeRepositoryManager)).withRouter(RoundRobinRouter(50)), "QueryHandlerRouter"
	)

	def receive = {
		case QueryDispatchRequest(rawQuery, ctx) =>
			val queryPackage = TurbineQueryPackage(rawQuery)
			queryHandlerRouter ! HandleQuery(queryPackage, ctx)
		case _ =>
	}
}