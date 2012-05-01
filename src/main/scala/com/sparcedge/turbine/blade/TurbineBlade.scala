package com.sparcedge.turbine.blade

import akka.actor.{ActorSystem,Actor,Props}
import com.sparcedge.turbine.blade.config.BladeConfig

object TurbineBlade extends App {

	val configStr = args.headOption.getOrElse {
		throw new Exception("No Configuration Supplied")
	}

	val config = BladeConfig.parse(configStr)
	val actorSystem = ActorSystem("TurbineBladeActorSystem")
	val bladeManager = actorSystem.actorOf(Props[TurbineBladeManager], name = "BladeManager")

	println("Turbine Blade: running")

	//for( rawQuery <- io.Source.stdin.getLines ) {
	//	bladeManager ! QueryDispatchRequest(rawQuery)
	//}

	Iterator.continually(Console.readLine) takeWhile(_ != "") foreach { rawQuery => 
		bladeManager ! QueryDispatchRequest(rawQuery)
	}

	actorSystem.shutdown
}