package com.sparcedge.turbine.blade.data

import akka.actor.{Actor,ActorRef}
import scala.util.{Try,Success,Failure}
import scala.concurrent.{future,ExecutionContext}
import com.sparcedge.turbine.blade.event.Event
import com.sparcedge.turbine.blade.query.{Blade,Match,Grouping}

object DataPartitionManager {

	case class WriteEvent(event: Event)

	case class PopulateIndexesRequest(indexes: Iterable[Index], reqSegments: Iterable[String], optSegments: Iterable[String], matches: Iterable[Match], groupings: Iterable[Grouping])
}

import AggregateIndex._
import DataPartitionManager._

class DataPartitionManager(blade: Blade) extends Actor {
	import context.dispatcher

	val partition = new DataPartition(blade)

	def receive = {
		case WriteEvent(event) =>
			partition.writeEvent(event)
		case PopulateIndexesRequest(indexes, reqSegments, optSegments, matches, groupings) =>
			val f = future { partition.populateIndexes(indexes, reqSegments, optSegments, matches, groupings) }
			f onComplete {
				case Success(un) => indexes.foreach(index => index.indexManager ! PopulatedIndex(index))
				case Failure(err) => // TODO: Handle Population Failure
			}
		case _ =>
	}
}