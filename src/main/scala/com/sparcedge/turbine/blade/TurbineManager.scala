package com.sparcedge.turbine.blade

import akka.actor.{Actor,Props,ActorSystem,ActorRef}
import akka.routing.RoundRobinRouter
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicLong
import com.sparcedge.turbine.blade.data.BladeManager
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.util.{DiskUtil,Timer}
import scala.collection.mutable
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import spray.routing.RequestContext

object TurbineManager {

	case class QueryDispatchRequest(rawQuery: String, ctx: RequestContext)
}

import TurbineManager._
import QueryHandler._
import BladeManager._

class TurbineManager(preloadBlades: List[Blade]) extends Actor {

	val bladeManagerMap = discoverExistingBladesAndInitializeNewBlades(preloadBlades)
	val queryHandlerRouter = context.actorOf(Props[QueryHandler].withRouter(RoundRobinRouter(50)), "QueryHandlerRouter")
	var bladeManagers = (bladeManagerMap.map(_._2)).toIndexedSeq
	val next = new AtomicLong(0)

	def receive = {
		case QueryDispatchRequest(rawQuery, ctx) =>
			val query = TurbineQuery(rawQuery)
			val bladeManager = bladeManagerMap.getOrElseUpdate(query.blade, {
				val man = context.actorOf(Props(new BladeManager(query.blade)), name = query.blade.toString)
				bladeManagers = bladeManagers :+ man
				man
			})
			queryHandlerRouter ! HandleQuery(query, bladeManager, ctx)
		case _ =>
	}

	def discoverExistingBladesAndInitializeNewBlades(blades: List[Blade]): mutable.Map[Blade,ActorRef] = {
		val timer = new Timer
		timer.start()
		val existingBlades = DiskUtil.retrieveBladesFromExistingData()
		timer.stop("[TurbineBladeManager] Discovered existing blades (" + existingBlades.size + ")")
		timer.start()
		val allBlades = (blades ++ existingBlades).toSet
		val bladeMap = mutable.Map ( 
			allBlades.toSeq map { blade =>
				(blade, context.actorOf(Props(new BladeManager(blade)), name = blade.toString))
			}: _*
		)
		timer.stop("[TurbineBladeManager] Created managers for all new / existing blades (" + allBlades.size + ")")
		bladeMap
	}
}