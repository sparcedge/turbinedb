package com.sparcedge.turbine.services

//import akka.config.Supervision._
//import akka.actor.{Supervisor, Actor}

object Main extends App {

	// create, start and supervise the TestService actor, which holds our custom request handling logic
	// as well as the HttpServer actor
	//Supervisor (
	//	SupervisorConfig (
	//		OneForOneStrategy(List(classOf[Exception]), 3, 100),
	//		List (
	//			Supervise(Actor.actorOf(new TurbineService()), Permanent),
	//			Supervise(Actor.actorOf(new HttpServer()), Permanent)
	//		)
	//	)
	//)
}