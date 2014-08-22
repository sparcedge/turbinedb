package com.sparcedge.turbine.ejournal

import scala.collection.mutable
import akka.actor.{Actor,ActorRef,Props,ActorLogging}
import akka.routing.RoundRobinPool
import journal.io.api.{Journal,Location}
import Journal.WriteType
import spray.routing.RequestContext
import spray.httpx.marshalling.BasicMarshallers

import com.sparcedge.turbine.event.{EventPackage,EventIngressPackage}
import com.sparcedge.turbine.behaviors.BatchBehavior

object JournalWriter {
	case class WriteEventToJournal(eventIngressPkg: EventIngressPackage, ctx: RequestContext)
	case class WriteEventsToJournal(eventIngressPkg: Iterable[EventIngressPackage], ctx: RequestContext)
}

import JournalWriter._
import HttpResponder._

class JournalWriter(journal: Journal) extends Actor with BatchBehavior with ActorLogging {

  val responder = context.actorOf (
  	RoundRobinPool(50).props(Props[HttpResponder]).withDispatcher("com.sparcedge.turbinedb.http-dispatcher"),
  	"HttpResponderRouter"
  )

	val maxBatchSize = context.system.settings.config.getInt("com.sparcedge.turbinedb.journal.max-unsynced-events")
	val maxTimeUnflushed = context.system.settings.config.getInt("com.sparcedge.turbinedb.journal.max-time-unsynced")
	var unacknowledgedContexts = mutable.ArrayBuffer[RequestContext]()

	def receive = batchReceive orElse {
		case WriteEventToJournal(eventIngressPkg, ctx) =>
			writeEventPackageToJournal(eventIngressPkg, ctx)
		case WriteEventsToJournal(eventIngressPkgs, ctx) =>
			writeEventPackagesToJournal(eventIngressPkgs, ctx)
		case _ =>
	}

	def writeEventPackagesToJournal(eventPkgs: Iterable[EventIngressPackage], ctx: RequestContext) {
		eventPkgs foreach { pkg =>
			val serialized = EventIngressPackage.toBytes(pkg)
			journal.write(serialized, WriteType.ASYNC)			
		}
		unacknowledgedContexts += ctx
		eventPkgs.foreach(_ => incrementBatchSize())
	}

	def writeEventPackageToJournal(eventPkg: EventIngressPackage, ctx: RequestContext) {
		val serialized = EventIngressPackage.toBytes(eventPkg)
		journal.write(serialized, WriteType.ASYNC)
		unacknowledgedContexts += ctx
		incrementBatchSize()
	}

	def flushBatch() {
		log.debug("Syncing Journal to Disk and Completing Requests")
		journal.sync()
		acknowledgeAndClearRequests()
	}

	def acknowledgeAndClearRequests() {
		responder ! RespondMultiple(unacknowledgedContexts, """{ "ok": true }""")
		unacknowledgedContexts = mutable.ArrayBuffer[RequestContext]()
	}
}

object HttpResponder {
	case class Respond(ctx: RequestContext, resp: String)
	case class RespondMultiple(ctxs: Iterable[RequestContext], resp: String)
}

class HttpResponder extends Actor with BasicMarshallers {
	def receive = {
		case Respond(ctx, resp) =>
			ctx.complete(resp)
		case RespondMultiple(ctxs, resp) =>
			ctxs.foreach(_.complete(resp))
	}
}