package com.sparcedge.turbine

import java.io.File
import scala.util.{Try,Success,Failure}
import akka.actor.{Actor,Props,ActorSystem,ActorRef,ActorLogging}
import akka.routing.FromConfig
import spray.routing.RequestContext
import journal.io.api.Journal

import com.sparcedge.turbine.event.{EventIngressPackage,IngressEvent}
import com.sparcedge.turbine.data.{BladeManager,WriteHandler}
import com.sparcedge.turbine.ejournal.{JournalReader,JournalWriter}
import com.sparcedge.turbine.query.{TurbineQueryPackage,TurbineQuery,QueryHandler}

object TurbineManager {
	case class QueryDispatchRequest(queryPkg: TurbineQueryPackage, ctx: RequestContext)
	case class AddEventRequest(eventIngressPkg: EventIngressPackage, ctx: RequestContext)
	case class AddEventsRequest(eventIngressPkgs: Iterable[EventIngressPackage], ctx: RequestContext)
	case class DescribeInstanceRequest(ctx: RequestContext)
	case class DescribeDatabaseRequest(database: String, ctx: RequestContext)
	case class DescribeCollectionRequest(collection: Collection, ctx: RequestContext)
	var eventsWrittenListener: Option[ActorRef] = null
}

import TurbineManager._
import QueryHandler._
import BladeManager._
import JournalWriter._
import MetaManager._

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

	val metaManager = context.actorOf (
		Props(newMetaManager(bladeRepositoryManager)), "MetaManager"
	)

	eventsWrittenListener = Some(journalReader)

	def receive = {
		case QueryDispatchRequest(queryPkg, ctx) =>
			queryHandlerRouter ! HandleQuery(queryPkg, ctx)
		case AddEventRequest(eventIngressPkg, ctx) =>
			journalWriter ! WriteEventToJournal(eventIngressPkg, ctx)
		case AddEventsRequest(eventIngressPkgs, ctx) =>
			journalWriter ! WriteEventsToJournal(eventIngressPkgs, ctx)
		case DescribeInstanceRequest(ctx) =>
			println(s"Describe Instance")
			metaManager ! DescribeDatabases(ctx)
		case DescribeDatabaseRequest(database, ctx) =>
			println(s"Describe Database: ${database}")
			metaManager ! DescribeCollections(database, ctx)
		case DescribeCollectionRequest(collection, ctx) =>
			println(s"Describe Collection: ${collection}")
			metaManager ! DescribeSegments(collection, ctx)
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
	def newMetaManager(bladeManRepo: ActorRef): Actor = new MetaManager(bladeManRepo)

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