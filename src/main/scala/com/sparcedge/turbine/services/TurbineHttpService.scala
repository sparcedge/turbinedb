package com.sparcedge.turbine.services

import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor._
import spray.routing.{HttpService, RequestContext}
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._

import com.sparcedge.turbine.TurbineManager._

class TurbineHttpServiceActor(val turbineManager: ActorRef) extends Actor with TurbineHttpService {
	def actorRefFactory = context
	def receive = runRoute(turbineRoute)
}

trait TurbineHttpService extends HttpService {
	val turbineManager: ActorRef

	val turbineRoute = {
		path("") {
			get {
				complete {
					"I am the turbine, goo goo g'joob"
				}
			}
		} ~
		path("query") {
			post {	
				entity(as[String]) { rawQuery =>
					respondWithMediaType(`application/json`) { ctx =>
						turbineManager ! QueryDispatchRequest(rawQuery, ctx)
					}
				}
			}
		} ~
		path("events") {
			post {
				entity(as[String]) { rawEvent =>
					respondWithMediaType(`application/json`) { ctx =>
						turbineManager ! AddEventRequest(rawEvent, ctx)
					}
				}
			}
		}
	}
}