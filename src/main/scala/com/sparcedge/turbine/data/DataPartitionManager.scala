package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,ActorLogging}
import scala.util.{Try,Success,Failure}
import scala.concurrent.{future,ExecutionContext}

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.query.{Match,Grouping}
import com.sparcedge.turbine.ejournal.JournalReader
import com.sparcedge.turbine.behaviors.BatchStorage
import com.sparcedge.turbine.{TurbineManager,Blade}

object DataPartitionManager {
	case class WriteEvent(id: String, event: Event)
	case class PopulateIndexesRequest(indexes: Iterable[Index])
	case class PartitionSegmentsRequest()
	case class PartitionSegmentsResponse(segments: Iterable[String])
}

import AggregateIndex._
import DataPartitionManager._
import JournalReader._

class DataPartitionManager(blade: Blade) extends Actor with ActorLogging with BatchStorage[(String,Event)] { this: DataPartitionManagerProvider =>
	import context.dispatcher

	val partition = newDataPartition(blade)
	log.info("Created data partition: {}", blade.key)
	val maxBatchSize = context.system.settings.config.getInt("com.sparcedge.turbinedb.data.partition.max-batched-events")
	val maxTimeUnflushed = context.system.settings.config.getInt("com.sparcedge.turbinedb.data.partition.max-time-batched")

	def receive = batchReceive orElse {
		case WriteEvent(id, event) =>
			addToBatch((id,event))			
		case PopulateIndexesRequest(indexes) =>
			val f = future { partition.populateIndexes(indexes) }
			f onComplete {
				case Success(un) => 
					indexes.foreach(index => index.indexManager ! PopulatedIndex(index))
				case Failure(err) => // TODO: Handle Population Failure
			}
		case PartitionSegmentsRequest() =>
			sender ! PartitionSegmentsResponse(partition.dataSegments.filterNot(s => s == "ts" || s == "its"))
		case _ =>
	}

	def flushBatch(batch: Iterable[(String,Event)]) {
		log.debug("Writing {} events to disk ({})", batch.size, blade.key)
		partition.writeEvents(batch.map(_._2))
		TurbineManager.eventsWrittenListener.foreach { _ ! EventsWrittenToDisk(batch.map(_._1)) }
	}
}

trait DataPartitionManagerProvider {
	def newDataPartition(blade: Blade): DataPartition = new DataPartition(blade)
}