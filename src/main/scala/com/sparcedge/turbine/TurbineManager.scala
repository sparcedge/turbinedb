package com.sparcedge.turbine

import java.io.File
import scala.util.{Try,Success,Failure}
import akka.actor.{Actor,Props,ActorSystem,ActorRef,ActorLogging}
import akka.routing.FromConfig
import spray.routing.RequestContext
import spray.http.{HttpResponse,HttpEntity,StatusCodes}
import journal.io.api.Journal

import com.sparcedge.turbine.event.{EventIngressPackage,IngressEvent}
import com.sparcedge.turbine.data.{BladeManager,WriteHandler}
import com.sparcedge.turbine.ejournal.{JournalReader,JournalWriter}
import com.sparcedge.turbine.query.{TurbineQueryPackage,TurbineQuery,QueryHandler}

object TurbineManager {
	case class QueryDispatchRequest(rawQuery: String, collection: Collection, ctx: RequestContext)
	case class AddEventRequest(rawEvent: String, collection: Collection, ctx: RequestContext)
	// TODO: Nasty Nasty Hack
	var universalEventWrittenListener: ActorRef = null
}

import TurbineManager._
import QueryHandler._
import BladeManager._
import JournalWriter._

class TurbineManager() extends Actor with ActorLogging { this: TurbineManagerProvider =>

	val journal = newInitializedJournal

	val bladeRepositoryManager = context.actorOf(Props(newBladeManagerRepository), "BladeRepositoryManager")
	log.info("Created BladeManagerRepository")

	val writeHandlerRouter = context.actorOf (
		Props(new WriteHandler(bladeRepositoryManager)).withRouter(FromConfig()), "WriteHandlerRouter"
	)
	log.info("Created WriteHandlerRouter")

	val queryHandlerRouter = context.actorOf (
		Props(new QueryHandler(bladeRepositoryManager)).withRouter(FromConfig()), "QueryHandlerRouter"
	)
	log.info("Created WriteHandlerRouter")

	val journalReader = context.actorOf (
		Props(newJournalReader(journal, writeHandlerRouter)), "JournalReader"
	)
	log.info("Created JournalReader")

	val journalWriter = context.actorOf (
		Props(newJournalWriter(journal)), "JournalWriter"
	)
	log.info("Created JournalWriter")

	universalEventWrittenListener = journalReader

	def receive = {
		case QueryDispatchRequest(rawQuery, collection, ctx) =>
			TurbineQuery.tryParse(rawQuery) match {
				case Success(query) =>
					val queryPackage = TurbineQueryPackage(collection, query)
					queryHandlerRouter ! HandleQuery(queryPackage, ctx)
				case Failure(err) =>
					log.error(err, "Failed parsing query from dispatch request")
					ctx.complete(HttpResponse(StatusCodes.InternalServerError))
			}
		case AddEventRequest(rawEvent, collection, ctx) =>
			IngressEvent.tryParse(rawEvent) match {
				case Success(ingressEvent) =>
					val eventIngressPkg = EventIngressPackage(collection, ingressEvent)
					journalWriter ! WriteEventToJournal(eventIngressPkg, ctx)
				case Failure(err) =>
					log.error(err, "Failed parsing event from add event request")
					ctx.complete(HttpResponse(StatusCodes.InternalServerError))
			}
		case _ =>
	}
}

trait TurbineManagerProvider { this: Actor with ActorLogging =>
	def newInitializedJournal: Journal = initializeJournal(new Journal)
	def newBladeManagerRepository: Actor = new BladeManagerRepository with BladeManagerRepositoryProvider
	def newWriteHandler(bladeManRepo: ActorRef): Actor = new WriteHandler(bladeManRepo)
	def newQueryHandler(bladeManRepo: ActorRef): Actor = new QueryHandler(bladeManRepo)
	def newJournalReader(journal: Journal, writeHandlerRouter: ActorRef): Actor = new JournalReader(journal, writeHandlerRouter)
	def newJournalWriter(journal: Journal): Actor = new JournalWriter(journal)

	def initializeJournal(journal: Journal): Journal = {
		val journalDir = context.system.settings.config.getString("com.sparcedge.turbinedb.journal.directory")
		ensureJournalDirectoryExists(journalDir)
		journal.setDirectory(new File(journalDir))
		journal.open()
		log.info("Intialized Journal at location: {}", journalDir)
		journal
	}

	def ensureJournalDirectoryExists(dir: String) {
		new File(dir).mkdirs()
	}
}