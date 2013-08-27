package com.sparcedge.turbine.services

import scala.util.{Try,Success,Failure}
import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor._
import spray.routing.{HttpService, RequestContext}
import spray.util._
import spray.http._
import HttpMethods._
import HttpHeaders._
import CacheDirectives._
import MediaTypes._

import com.sparcedge.turbine.event.{IngressEvent,EventIngressPackage}
import com.sparcedge.turbine.Collection
import com.sparcedge.turbine.query.{TurbineQuery,TurbineQueryPackage}

import com.sparcedge.turbine.TurbineManager._

class TurbineHttpServiceActor(val turbineManager: ActorRef) extends Actor with TurbineHttpService with SprayActorLogging {
	def actorRefFactory = context
	def receive = runRoute(turbineRoute)
	val streamingNotifier = context.actorOf(Props[StreamingNotifier], "streaming-notifier")
}

import StreamingNotifier._

trait TurbineHttpService extends HttpService { this: SprayActorLogging =>
	val turbineManager: ActorRef
	val streamingNotifier: ActorRef

	val turbineRoute = {
		path("") {
			get {
				complete {
					"I am the turbine, goo goo g'joob"
				}
			}
		} ~
		pathPrefix("bulk") {
			pathPrefix(Segment) { database =>
				pathPrefix(Segment) { collection =>
					post {
						entity(as[String]) { rawEvents =>
							respondWithMediaType(`application/json`) { ctx =>
								IngressEvent.tryParseMany(rawEvents) match {
									case Success(ingressEvents) =>
										val coll = Collection(database, collection)
										val eventIngressPkgs = ingressEvents.map(EventIngressPackage(coll, _))
										turbineManager ! AddEventsRequest(eventIngressPkgs, ctx)
										streamingNotifier ! StreamEventPackages(eventIngressPkgs)
									case Failure(err) =>
										log.error(err, "Failed parsing event from add event request")
										ctx.complete(HttpResponse(StatusCodes.InternalServerError))
								}
							}
						}
					}
				}
			}
		} ~
		pathPrefix("db" | "events") {
			pathPrefix(Segment) { database =>
				pathPrefix(Segment) { collection =>
					path("") {
						(get & parameter('q) ) { rawQuery =>
							respondWithMediaType(`application/json`) { ctx =>
								TurbineQuery.tryParse(rawQuery) match {
									case Success(query) =>
										val queryPackage = TurbineQueryPackage(Collection(database, collection), query)
										turbineManager ! QueryDispatchRequest(queryPackage, ctx)
									case Failure(err) =>
										log.error(err, "Failed parsing query from dispatch request")
										ctx.complete(HttpResponse(StatusCodes.InternalServerError))
								}
							}
						} ~
						post {
							entity(as[String]) { rawEvent =>
								respondWithMediaType(`application/json`) { ctx =>
									IngressEvent.tryParse(rawEvent) match {
										case Success(ingressEvent) =>
											val eventIngressPkg = EventIngressPackage(Collection(database, collection), ingressEvent)
											turbineManager ! AddEventRequest(eventIngressPkg, ctx)
											streamingNotifier ! StreamEventPackage(eventIngressPkg, rawEvent)
										case Failure(err) =>
											log.error(err, "Failed parsing event from add event request")
											ctx.complete(HttpResponse(StatusCodes.InternalServerError))
									}
								}
							}
						}
					}
				}
			}
		} ~
		pathPrefix("notify" / Segment) { database =>
			pathPrefix(Segment) { collection =>
				path("") {
					(get & parameter('m) ) { matchStr =>
						respondAsEventStream { ctx =>
							TurbineQuery.tryParseMatches(matchStr) match {
								case Success(matches) =>
									streamingNotifier ! NewListener(ctx, Collection(database,collection), matches)
								case Failure(err) =>
									log.error(err, "Failed parsing matches from notify request")
									ctx.complete(HttpResponse(StatusCodes.InternalServerError))
							}
						}
					}
				}
			}
		}
	}

	def respondAsEventStream = 
		respondWithHeader(`Cache-Control`(`no-cache`)) &
		respondWithHeader(`Connection`("Keep-Alive"))
}