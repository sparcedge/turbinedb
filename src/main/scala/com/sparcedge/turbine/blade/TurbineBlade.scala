package com.sparcedge.turbine.blade

import akka.actor.{ActorSystem,Actor,Props}

object TurbineBlade extends App {

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