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
		pathPrefix("db") {
			pathPrefix(PathElement) { domain =>
				pathPrefix(PathElement) { tenant =>
					pathPrefix(PathElement) { category =>
						path("") {
							(get & parameter('q) ) { query =>
								entity(as[String]) { rawQuery =>
									respondWithMediaType(`application/json`) { ctx =>
										turbineManager ! QueryDispatchRequest(query, domain, tenant, category, ctx)
									}
								}
							} ~
							post {
								entity(as[String]) { rawEvent =>
									respondWithMediaType(`application/json`) { ctx =>
										turbineManager ! AddEventRequest(rawEvent, domain, tenant, category, ctx)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}