package com.sparcedge.turbine.blade

import akka.actor.{ActorSystem,Actor,Props}
import com.sparcedge.turbine.blade.config.BladeConfig
import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.sparcedge.turbine.blade.utils.Timer

object TurbineBlade extends App {

	val configStr = args.headOption.getOrElse {
		throw new Exception("No Configuration Supplied")
	}

	val config = BladeConfig(configStr)
	val actorSystem = ActorSystem("TurbineBladeActorSystem")
	val mongoConnection = MongoDBConnection(config)
	val printTimings = config.printTimings.getOrElse(false)
	val preloadBlades = config.preloadBlades.getOrElse(List[Blade]())
	val dataDirectory = config.dataDirectory.getOrElse("data")

	val bladeManager = actorSystem.actorOf(Props(new TurbineBladeManager(mongoConnection)), name = "BladeManager", preloadBlades = preloadBlades)

	Timer.printTimings = printTimings
	BFFUtil.BASE_DIRECTORY = dataDirectory

	println("{\"status\": \"running\"}")

	for( rawQuery <- io.Source.fromInputStream(System.in)("UTF-8").getLines ) {
		bladeManager ! QueryDispatchRequest(rawQuery)
	}

	actorSystem.shutdown
}