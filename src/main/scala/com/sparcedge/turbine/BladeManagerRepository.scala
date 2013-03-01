package com.sparcedge.turbine

import akka.actor.{Actor,Props,ActorSystem,ActorRef}

import com.sparcedge.turbine.query.{Blade,Collection}
import com.sparcedge.turbine.data.BladeManager
import com.sparcedge.turbine.util.{WrappedTreeMap,DiskUtil}

object BladeManagerRepository {
	case class BladeManagerRangeRequest(coll: Collection, sPeriodOpt: Option[String], ePeriodOpt: Option[String])
	case class BladeManagerRequest(blade: Blade)
	case class BladeManagerGetOrCreateRequest(blade: Blade)
	
	case class BladeManagerRangeResponse(managers: Iterable[(Blade,ActorRef)])
	case class BladeManagerResponse(manager: Option[ActorRef])
	case class BladeManagerGetOrCreateResponse(manager: ActorRef)
}

import BladeManagerRepository._

class BladeManagerRepository() extends Actor {

	val bladeManagerMap = new WrappedTreeMap[String,(Blade,ActorRef)]()
	discoverAndInitializeExistingBlades()

	def receive = {
		case BladeManagerRequest(blade) =>
			sender ! BladeManagerResponse(bladeManagerMap.get(blade.key).map(_._2))
		case BladeManagerGetOrCreateRequest(blade) =>
			val (newBlade,manager) = bladeManagerMap.getOrElseUpdate(blade.key, (blade -> createManagerForBlade(blade)))
			sender ! BladeManagerGetOrCreateResponse(manager)
		case BladeManagerRangeRequest(coll, sPeriodOpt, ePeriodOpt) =>
			sender ! BladeManagerRangeResponse(getBladeManagersInRange(coll, sPeriodOpt, ePeriodOpt))
		case _ =>
	}

	def getBladeManagersInRange(coll: Collection, sPeriodOpt: Option[String], ePeriodOpt: Option[String]): Iterable[(Blade,ActorRef)] = {
		(sPeriodOpt, ePeriodOpt) match {
			case (Some(sPeriod), Some(ePeriod)) => getBladeManagersInRange(Blade(coll, sPeriod), Blade(coll, ePeriod))
			case (Some(sPeriod), _) => getBladeManagersInRange(Blade(coll, sPeriod), getUpperBoundaryForCollection(coll))
			case (_, Some(ePeriod)) => getBladeManagersInRange(getLowerBoundaryForCollection(coll), Blade(coll,ePeriod))
			case _ => getBladeManagersInRange(getLowerBoundaryForCollection(coll), getUpperBoundaryForCollection(coll))
		}
	}

	def getBladeManagersInRange(sBlade: Blade, eBlade: Blade): Iterable[(Blade,ActorRef)] = {
		println(s"StartBlade: ${sBlade}")
		println(s"EndBlade: ${eBlade}")
		val managers = bladeManagerMap.subMap(sBlade.key, eBlade.key).values
		println(s"Managers: ${managers.size}")
		managers
	}

	def getUpperBoundaryForCollection(coll: Collection): Blade = {
		Blade(coll.copy(category = coll.category+1), "0001-01")
	}

	def getLowerBoundaryForCollection(coll: Collection): Blade = {
		Blade(coll, "0001-01")
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