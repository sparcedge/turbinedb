package com.sparcedge.turbine

import java.io.File
import spray.can.server.SprayCanHttpServerApp
import akka.actor.Props
import akka.actor.{ActorSystem,Actor,Props}
import com.typesafe.config.ConfigFactory

import com.sparcedge.turbine.services.TurbineHttpServiceActor
import com.sparcedge.turbine.util.{Timer,DiskUtil}


object Main extends App with SprayCanHttpServerApp {

	val defaultConfig = ConfigFactory.load()
	val userConfig = for (
		configPath <- args.headOption;
		configFile = new File(configPath);
		config = ConfigFactory.parseFile(configFile).withFallback(defaultConfig)
	) yield config

	val appConfig = userConfig.getOrElse(defaultConfig)

	val actorSystem = ActorSystem("TurbineActorSystem", appConfig)
	val printTimings = appConfig.getBoolean("com.sparcedge.turbinedb.print-timings")
	val dataDirectory = appConfig.getString("com.sparcedge.turbinedb.data.directory")

	val bladeManager = actorSystem.actorOf(Props[TurbineManager], name = "BladeManager")

	Timer.printTimings = printTimings
	DiskUtil.BASE_PATH = dataDirectory

	val handler = system.actorOf(Props(new TurbineHttpServiceActor(bladeManager)).withDispatcher("com.sparcedge.turbinedb.http-dispatcher"))
	newHttpServer(handler) ! Bind(interface = "localhost", port = 8080)
}