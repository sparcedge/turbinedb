package com.sparcedge.turbine.services

import spray.can.server.SprayCanHttpServerApp
import akka.actor.Props
import akka.actor.{ActorSystem,Actor,Props}

import com.sparcedge.turbine.blade.TurbineBladeManager
import com.sparcedge.turbine.blade.config.BladeConfig
import com.sparcedge.turbine.blade.util.{Timer,DiskUtil}
import com.sparcedge.turbine.blade.query.Blade


object Main extends App with SprayCanHttpServerApp {

	val configStr = args.headOption.getOrElse {
		throw new Exception("No Configuration Supplied")
	}

	val config = BladeConfig(configStr)
	val actorSystem = ActorSystem("TurbineBladeActorSystem")
	val printTimings = config.printTimings.getOrElse(false)
	val preloadBlades = config.preloadBlades.getOrElse(List[Blade]())
	val dataDirectory = config.dataDirectory.getOrElse("data")

	val bladeManager = actorSystem.actorOf(Props(new TurbineBladeManager(preloadBlades)), name = "BladeManager")

	Timer.printTimings = printTimings
	DiskUtil.BASE_PATH = dataDirectory

	// the handler actor replies to incoming HttpRequests
	val handler = system.actorOf(Props(new TurbineHttpServiceActor(bladeManager)))

	// create a new HttpServer using our handler and tell it where to bind to
	newHttpServer(handler) ! Bind(interface = "localhost", port = 8080)
}