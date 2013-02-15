package com.sparcedge.turbine.data

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.{ExecutionContext,Await,Future}
import scala.concurrent.duration._
import scala.util.{Try,Success,Failure}
import scala.collection.mutable
import spray.routing.RequestContext
import spray.http.{HttpResponse,HttpEntity,StatusCodes}

import com.sparcedge.turbine.event.{EventPackage,Event}
import com.sparcedge.turbine.BladeManagerRepository
import com.sparcedge.turbine.util.{WrappedTreeMap,CustomJsonSerializer}
import com.sparcedge.turbine.data._
import com.sparcedge.turbine.query.Blade

object WriteHandler {
	case class WriteEventRequest(eventPkg: EventPackage)
}

import BladeManagerRepository._
import BladeManager._
import WriteHandler._

class WriteHandler(bladeManagerRepository: ActorRef) extends Actor {

	implicit val timeout = Timeout(240 seconds)
	implicit val ec: ExecutionContext = context.dispatcher

	def receive = {
		case WriteEventRequest(eventPkg) =>
			// IN THE FUTURE!!!
			val manager = retrieveAndOrCreateManager(eventPkg.blade)
			writeEvent(manager, eventPkg.event)
		case _ =>
	}

	def retrieveAndOrCreateManager(blade: Blade): Future[ActorRef] = {
		val bladeManResponseFuture = (bladeManagerRepository ? BladeManagerGetOrCreateRequest(blade)).mapTo[BladeManagerGetOrCreateResponse]
		bladeManResponseFuture.map { response => response.manager }
	}

	def writeEvent(manager: Future[ActorRef], event: Event) {
		manager.foreach { man => man ! AddEvent(event) }
	}
}