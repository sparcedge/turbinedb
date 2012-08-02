package com.sparcedge.turbine.blade

import akka.actor.{ActorSystem,Actor,Props}
import com.sparcedge.turbine.blade.config.BladeConfig
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.query.cache.Timer

object TurbineBlade extends App {

	val configStr = args.headOption.getOrElse {
		throw new Exception("No Configuration Supplied")
	}

	val config = BladeConfig(configStr)
	val actorSystem = ActorSystem("TurbineBladeActorSystem")
	val mongoConnection = MongoDBConnection(config)
	val bladeManager = actorSystem.actorOf(Props(new TurbineBladeManager(mongoConnection)), name = "BladeManager")
	val printTimings = config.printTimings match {
		case Some(yesNo) => yesNo
		case None => false
	}
	Timer.printTimings = printTimings

	println("{\"status\": \"running\"}")

	for( rawQuery <- io.Source.fromInputStream(System.in)("UTF-8").getLines ) {
		bladeManager ! QueryDispatchRequest(rawQuery)
	}

	actorSystem.shutdown
}