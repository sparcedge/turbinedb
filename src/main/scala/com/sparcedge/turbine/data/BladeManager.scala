package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,Props,ActorLogging}
import akka.pattern.{ask,pipe}
import akka.util.Timeout
import scala.collection.mutable
import scala.util.Random
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.sparcedge.turbine.query._
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.Blade

object BladeManager {
	case class IndexesRequest(query: TurbineQuery)
	case class IndexesResponse(indexes: Iterable[ActorRef])
	case class AddEvent(id: String, event: Event)
	case class SegmentsRequest()
	case class SegmentsResponse(segments: Iterable[String])
}

import BladeManager._
import DataPartitionManager._
import AggregateIndex._

class BladeManager(blade: Blade) extends Actor with ActorLogging { this: BladeManagerProvider =>

	implicit val timeout = Timeout(240.seconds)
	implicit val ec: ExecutionContext = context.dispatcher

	val partitionManager = context.actorOf (
		Props(newDataPartitionManager(blade)).withDispatcher("com.sparcedge.turbinedb.data-partition-dispatcher"), 
		s"${blade.toString}-partition-manager"
	)

	val indexMap = mutable.Map[String,ActorRef]()

	def receive = {
		case IndexesRequest(query) =>
			val newIndexes = mutable.ListBuffer[Index]()

			val indexes = retrieveIndexKeysFromQuery(query) map { key => 
				indexMap.getOrElseUpdate(key.id, createAggregateIndex(key, newIndexes))
			}
			if(newIndexes.size > 0) {
				beginIndexPopulation(newIndexes)
			}
			sender ! IndexesResponse(indexes)
		case AddEvent(id, event) =>
			partitionManager ! WriteEvent(id, event)
			indexMap.values.foreach(_ ! UpdateIndex(event))
		case SegmentsRequest() =>
			val respondTo = sender
			(partitionManager ? PartitionSegmentsRequest())
				.mapTo[PartitionSegmentsResponse]
				.map(res => SegmentsResponse(res.segments))
				.pipeTo(respondTo)
		case _ =>
	}

	def retrieveIndexKeysFromQuery(query: TurbineQuery): Iterable[IndexKey] = {
		query.reducers map { reducerPkg => query.createAggregateIndexKey(reducerPkg.reducer) }
	}

	def createAggregateIndex(key: IndexKey, newIndexes: mutable.ListBuffer[Index]): ActorRef = {
		log.debug("Creating new index: {}", key.id)
		val indexActor = context.actorOf(Props(newAggregateIndex(key, blade)).withDispatcher("com.sparcedge.turbinedb.agg-index-dispatcher"), key.id)
		val index = new Index(key, indexActor, blade)
		index +=: newIndexes
		indexActor
	}

	def beginIndexPopulation(indexes: Iterable[Index]) {
		partitionManager ! PopulateIndexesRequest(indexes)
	}
}

case class IndexKey (reducer: Reducer, extenders: Iterable[Extend], matches: Iterable[Match], groupings: Iterable[Grouping]) {
	val uniqueExtendStr = extenders.map(_.uniqueId).toList.mkString(".")
	val uniqueMatchStr = matches.map(_.uniqueId).toList.sorted.mkString(".")
	val uniqueGroupStr = groupings.map(_.uniqueId).toList.mkString(".")
	val id = removeSpaces(s"${reducer.reduceType}.${reducer.segment}.${uniqueExtendStr}.${uniqueMatchStr}.${uniqueGroupStr}")

	def removeSpaces(str: String): String = str.replaceAll("""\s""","")
}

trait BladeManagerProvider {
	def newDataPartitionManager(blade: Blade): Actor = new DataPartitionManager(blade) with DataPartitionManagerProvider
	def newAggregateIndex(key: IndexKey, blade: Blade): Actor = new AggregateIndex(key, blade)
}