package com.sparcedge.turbine.blade

import akka.actor.{ActorSystem,Actor,Props}
import com.sparcedge.turbine.blade.config.BladeConfig
import com.sparcedge.turbine.blade.mongo.MongoDBConnection

object TurbineBlade extends App {

	val configStr = "{\"mongo\": {\"servers\": [{\"host\": \"127.0.0.1\",\"port\": 27017}],\"database\": \"sparc-platform-dev\",\"collection\": \"events\"}}" //args.headOption.getOrElse {
		//throw new Exception("No Configuration Supplied")
	//}

	val config = BladeConfig(configStr)
	val actorSystem = ActorSystem("TurbineBladeActorSystem")
	val mongoConnection = MongoDBConnection(config)
	val bladeManager = actorSystem.actorOf(Props(new TurbineBladeManager(mongoConnection)), name = "BladeManager")

	println("Turbine Blade: running")

	for( rawQuery <- io.Source.stdin.getLines ) {
		bladeManager ! QueryDispatchRequest(rawQuery)
	}

	actorSystem.shutdown
}