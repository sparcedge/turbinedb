package com.sparcedge.turbine

import akka.actor.{Actor,Props,ActorSystem,ActorRef}

import com.sparcedge.turbine.query.Blade
import com.sparcedge.turbine.data.BladeManager
import com.sparcedge.turbine.util.{WrappedTreeMap,Timer,DiskUtil}

object BladeManagerRepository {
	case class BladeManagerRangeRequest(sBlade: Blade, eBlade: Blade)
	case class BladeManagerRangeUnboundedRequest(sBlade: Blade)
	case class BladeManagerRequest(blade: Blade)
	case class BladeManagerGetOrCreateRequest(blade: Blade)
	
	case class BladeManagerRangeResponse(managers: Iterable[(Blade,ActorRef)])
	case class BladeManagerResponse(manager: Option[ActorRef])
	case class BladeManagerGetOrCreateResponse(manager: ActorRef)
}

import BladeManagerRepository._

class BladeManagerRepository extends Actor {

	val bladeManagerMap = new WrappedTreeMap[String,(Blade,ActorRef)]()
	discoverAndInitializeExistingBlades()

	def receive = {
		case BladeManagerRequest(blade) =>
			sender ! BladeManagerResponse(bladeManagerMap.get(blade.key).map(_._2))
		case BladeManagerGetOrCreateRequest(blade) =>
			val (newBlade,manager) = bladeManagerMap.getOrElseUpdate(blade.key, (blade -> createManagerForBlade(blade)))
			sender ! BladeManagerGetOrCreateResponse(manager)
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

	def discoverAndInitializeExistingBlades() {
		val existingBlades = DiskUtil.retrieveBladesFromExistingData()
		initializeBlades(existingBlades)
	}

	def initializeBlades(blades: Iterable[Blade]) = {
		blades foreach { blade =>
			bladeManagerMap(blade.key) = (blade -> createManagerForBlade(blade))
		}
	}

	def createManagerForBlade(blade: Blade): ActorRef = {
		context.actorOf(Props(new BladeManager(blade)), name = blade.toString)
	}
}