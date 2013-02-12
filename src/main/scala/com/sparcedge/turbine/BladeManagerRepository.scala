package com.sparcedge.turbine

import akka.actor.{Actor,Props,ActorSystem,ActorRef}

import com.sparcedge.turbine.query.Blade
import com.sparcedge.turbine.data.BladeManager
import com.sparcedge.turbine.util.{WrappedTreeMap,Timer,DiskUtil}

object BladeManagerRepository {

	case class BladeManagerRangeRequest(sBlade: Blade, eBlade: Blade)
	case class BladeManagerRangeUnboundedRequest(sBlade: Blade)
	case class BladeManagerRequest(blade: Blade)
	case class BladeManagerRangeResponse(managers: Iterable[(Blade,ActorRef)])
	case class BladeManagerResponse(manager: Option[ActorRef])
}

import BladeManagerRepository._

class BladeManagerRepository(preloadBlades: Iterable[Blade]) extends Actor {

	val bladeManagerMap = discoverExistingBladesAndInitializeNewBlades(preloadBlades)

	def receive = {
		case BladeManagerRequest(blade) =>
			sender ! BladeManagerResponse(bladeManagerMap.get(blade.key).map(_._2))
		case BladeManagerRangeRequest(sBlade, eBlade) =>
			sender ! BladeManagerRangeResponse(getBladeManagersInRange(sBlade, eBlade))
		case BladeManagerRangeUnboundedRequest(sBlade) =>
			sender ! BladeManagerRangeResponse(getBladeManagersInUnboundedRange(sBlade))
		case _ =>
	}

	def getBladeManagersInUnboundedRange(sBlade: Blade): Iterable[(Blade,ActorRef)] = {
		getBladeManagersInRange(sBlade, sBlade.copy(category = sBlade.category+1, period = "0001-01"))
	}

	def getBladeManagersInRange(sBlade: Blade, eBlade: Blade): Iterable[(Blade,ActorRef)] = {
		bladeManagerMap.subMap(sBlade.key, eBlade.key).values
	}

	def discoverExistingBladesAndInitializeNewBlades(blades: Iterable[Blade]): WrappedTreeMap[String,(Blade,ActorRef)] = {
		val timer = new Timer

		timer.start()
		val managerMap = new WrappedTreeMap[String,(Blade,ActorRef)]()
		val existingBlades = DiskUtil.retrieveBladesFromExistingData()
		timer.stop("[TurbineBladeManager] Discovered existing blades (" + existingBlades.size + ")")

		timer.start()
		val allBlades = (blades ++ existingBlades).toSet
		allBlades foreach { blade =>
			managerMap(blade.key) = (blade -> createManagerForBlade(blade))
		}
		timer.stop("[TurbineBladeManager] Created managers for all new / existing blades (" + allBlades.size + ")")

		managerMap
	}

	def createManagerForBlade(blade: Blade): ActorRef = {
		context.actorOf(Props(new BladeManager(blade)), name = blade.toString)
	}
}