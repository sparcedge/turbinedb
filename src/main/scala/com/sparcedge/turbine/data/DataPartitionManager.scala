package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef}
import scala.util.{Try,Success,Failure}
import scala.concurrent.{future,ExecutionContext}
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.query.{Blade,Match,Grouping}
import com.sparcedge.turbine.ejournal.JournalReader
import com.sparcedge.turbine.{BatchStorage,TurbineManager}

object DataPartitionManager {
	case class WriteEvent(id: String, event: Event)
	case class PopulateIndexesRequest(indexes: Iterable[Index])
}

import AggregateIndex._
import DataPartitionManager._
import JournalReader._

class DataPartitionManager(blade: Blade) extends Actor with BatchStorage[(String,Event)] {
	import context.dispatcher

	val partition = new DataPartition(blade)
	lazy val eventListener = TurbineManager.universalEventWrittenListener
	val maxBatchSize = context.system.settings.config.getInt("com.sparcedge.turbinedb.data.partition.max-batched-events")
	val maxTimeUnflushed = context.system.settings.config.getInt("com.sparcedge.turbinedb.data.partition.max-time-batched")

	def receive = batchReceive orElse {
		case WriteEvent(id, event) =>
			addToBatch((id,event))			
		case PopulateIndexesRequest(indexes) =>
			val f = future { partition.populateIndexes(indexes) }
			f onComplete {
				case Success(un) => indexes.foreach(index => index.indexManager ! PopulatedIndex(index))
				case Failure(err) => // TODO: Handle Population Failure
			}
		case _ =>
	}

	def flushBatch(batch: Iterable[(String,Event)]) {
		partition.writeEvents(batch.map(_._2))
		eventListener ! EventsWrittenToDisk(batch.map(_._1))
	}
}