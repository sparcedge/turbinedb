package com.sparcedge.turbine.ejournal

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Actor,ActorRef,Props}
import scala.concurrent.duration._
import journal.io.api.{Journal,Location}
import Journal.ReadType

import com.sparcedge.turbine.event.EventPackage
import com.sparcedge.turbine.data.WriteHandler

object JournalReader {
	case class EventWrittenToDisk(id: String)
	case object ProcessJournalEvents
}

import JournalReader._
import WriteHandler._

class JournalReader(journal: Journal, writeHandlerRouter: ActorRef) extends Actor {

	val eventLocations = mutable.Map[String,Location]()
	val processJournalDelay = context.system.settings.config.getInt("com.sparcedge.turbinedb.journal.process-delay")
	var lastLocation: Option[Location] = None
	scheduleProcessJournalMessage()

	var processedAndAcknowledged = 0

	def receive = {
		case EventWrittenToDisk(id) =>
			removeEventFromJournal(id)
			processedAndAcknowledged += 1
		case ProcessJournalEvents =>
			processJournalEvents()
		case _ =>
	}

	def removeEventFromJournal(id: String) {
		eventLocations.remove(id) foreach { location =>
			journal.delete(location)
		}
	}

	def processJournalEvents() {
		val journalIterator = lastLocation map { ll => journal.redo(ll) } getOrElse { journal.redo() }
		journalIterator foreach { loc =>
			val locKey = createLocationKey(loc)
			val eventBytes = journal.read(loc, ReadType.ASYNC)
			writeHandlerRouter ! WriteEventRequest(locKey, eventBytes)
			eventLocations(locKey) = loc
			lastLocation = Some(loc)
		}
		scheduleProcessJournalMessage()
	}

	def scheduleProcessJournalMessage() {
		context.system.scheduler.scheduleOnce(processJournalDelay milliseconds, self, ProcessJournalEvents)
	}

	def createLocationKey(loc: Location): String = {
		s"${loc.getDataFileId}-${loc.getPointer}"
	}
}