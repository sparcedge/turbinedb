package com.sparcedge.turbine

import java.io.File
import akka.actor.Props
import akka.actor.{ActorSystem,Actor,Props}
import akka.io.IO
import spray.can.Http
import akka.event.Logging
import com.typesafe.config.ConfigFactory

import com.sparcedge.turbine.services.TurbineHttpServiceActor
import com.sparcedge.turbine.util.{Timer,DiskUtil}
import com.sparcedge.turbine.data.QueryUtil
import com.sparcedge.turbine.query.IndexGrouping


object Main extends App {

	val defaultConfig = ConfigFactory.load()
	val userConfig = for (
		configPath <- args.headOption;
		configFile = new File(configPath);
		config = ConfigFactory.parseFile(configFile).withFallback(defaultConfig)
	) yield config

	val appConfig = userConfig.getOrElse(defaultConfig).resolve()

	implicit val actorSystem = ActorSystem("TurbineActorSystem", appConfig)
	val printTimings = appConfig.getBoolean("com.sparcedge.turbinedb.print-timings")
	val dataDirectory = appConfig.getString("com.sparcedge.turbinedb.data.directory")
	val indexResolution = appConfig.getString("com.sparcedge.turbinedb.data.index-resolution")
	val httpInterface = appConfig.getString("com.sparcedge.turbinedb.http-interface")
	val serverPort = appConfig.getInt("com.sparcedge.turbinedb.port")
	val logger = Logging(actorSystem, "Turbine-Main");

	Timer.printTimings = printTimings
	DiskUtil.BASE_PATH = dataDirectory
	logger.info("Using DataDirectory: {}", dataDirectory)

	val turbineManager = actorSystem.actorOf(Props(new TurbineManager with TurbineManagerProvider), name = "TurbineManager")
	logger.info("Created Turbine Manager")

	val handler = actorSystem.actorOf(Props(new TurbineHttpServiceActor(turbineManager)).withDispatcher("com.sparcedge.turbinedb.http-dispatcher"))
	IO(Http) ! Http.Bind(handler, interface = httpInterface, port = serverPort)
	logger.info("Started Turbine Http Server")
}