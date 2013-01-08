package com.sparcedge.turbine.services

import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor._
import spray.routing.{HttpService, RequestContext}
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._

import com.sparcedge.turbine.blade.QueryDispatchRequest

class TurbineHttpServiceActor(val bladeManager: ActorRef) extends Actor with TurbineHttpService {
	def actorRefFactory = context
	def receive = runRoute(turbineRoute)
}

trait TurbineHttpService extends HttpService {
	val bladeManager: ActorRef

	val turbineRoute = {
		path("") {
			get {
				complete {
					"I am the turbine, goo goo g'joob"
				}
			}
		} ~
		post {
			path("query") { 
				entity(as[String]) { rawQuery =>
					respondWithMediaType(`application/json`) { ctx =>
						bladeManager ! QueryDispatchRequest(rawQuery, ctx)
					}
				}
			}
		}
	}	
}