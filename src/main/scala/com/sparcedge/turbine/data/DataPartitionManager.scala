package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef}
import scala.util.{Try,Success,Failure}
import scala.concurrent.{future,ExecutionContext}
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.query.{Blade,Match,Grouping}

object DataPartitionManager {
	case class WriteEvent(event: Event)
	case class PopulateIndexesRequest(indexes: Iterable[Index])
}

import AggregateIndex._
import DataPartitionManager._

class DataPartitionManager(blade: Blade) extends Actor {
	import context.dispatcher

	val partition = new DataPartition(blade)

	def receive = {
		case WriteEvent(event) =>
			partition.writeEvent(event)
		case PopulateIndexesRequest(indexes) =>
			val f = future { partition.populateIndexes(indexes) }
			f onComplete {
				case Success(un) => indexes.foreach(index => index.indexManager ! PopulatedIndex(index))
				case Failure(err) => // TODO: Handle Population Failure
			}
		case _ =>
	}
}