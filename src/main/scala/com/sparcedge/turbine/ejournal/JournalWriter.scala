package com.sparcedge.turbine.ejournal

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Actor,ActorRef,Props,Cancellable}
import journal.io.api.{Journal,Location}
import Journal.WriteType
import spray.routing.RequestContext

import com.sparcedge.turbine.event.{EventPackage,EventIngressPackage}

object JournalWriter {
	case class WriteEventToJournal(eventIngressPkg: EventIngressPackage, ctx: RequestContext)
	case object DiskSync
}

import JournalWriter._

class JournalWriter(journal: Journal) extends Actor {

	val maxUnsyncedEvents = context.system.settings.config.getInt("com.sparcedge.turbinedb.journal.max-unsynced-events")
	val maxTimeUnsynced = context.system.settings.config.getInt("com.sparcedge.turbinedb.journal.max-time-unsynced")

	var unsyncedEventCount = 0
	var unacknowledgedRequests = mutable.ArrayBuffer[RequestContext]()
	var scheduledSync: Option[Cancellable] = None

	def receive = {
		case WriteEventToJournal(eventIngressPkg, ctx) =>
			writeEventPackageToJournal(eventIngressPkg, ctx)
		case DiskSync =>
			syncToDiskAndAcknowledgeRequests()
			scheduledSync = None
		case _ =>
	}

	def writeEventPackageToJournal(eventPkg: EventIngressPackage, ctx: RequestContext) {
		val serialized = EventIngressPackage.toBytes(eventPkg) // TODO: Someone else
		journal.write(serialized, WriteType.ASYNC)
		unacknowledgedRequests += ctx
		unsyncedEventCount += 1

		if(unsyncedEventCount >= maxUnsyncedEvents) {
			syncToDiskAndAcknowledgeRequests()
			scheduledSync.foreach(_.cancel())
			scheduledSync = None
		} else if(scheduledSync == None) {
			scheduleJournalSync()
		}
	}

	def scheduleJournalSync() {
		scheduledSync = Some(context.system.scheduler.scheduleOnce(maxTimeUnsynced milliseconds, self, DiskSync))
	}

	def syncToDiskAndAcknowledgeRequests() {
		journal.sync()
		unsyncedEventCount = 0
		acknowledgeAndClearRequests()
	}

	def acknowledgeAndClearRequests() {
		try {
			unacknowledgedRequests.foreach(acknowledgeRequest) // TODO: Maybe Future Here!
		} catch {
			case e: Exception => // TODO: Handle
		}
		unacknowledgedRequests = mutable.ArrayBuffer[RequestContext]()
	}

	def acknowledgeRequest(ctx: RequestContext) {
		ctx.complete(""" {"ok": true} """)
	}
}