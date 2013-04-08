package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,Props,ActorLogging}
import scala.collection.mutable
import scala.util.Random

import com.sparcedge.turbine.query._
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.Blade

object BladeManager {
	case class IndexesRequest(query: TurbineQuery)
	case class IndexesResponse(indexes: Iterable[ActorRef])
	case class AddEvent(id: String, event: Event)
}

import BladeManager._
import DataPartitionManager._
import AggregateIndex._

class BladeManager(blade: Blade) extends Actor with ActorLogging { this: BladeManagerProvider =>

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
			// TODO: Efficiency
			indexMap.values.foreach(_ ! UpdateIndex(event))
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
	val id = s"${reducer.reduceType}.${reducer.segment}.${uniqueExtendStr}.${uniqueMatchStr}.${uniqueGroupStr}"
}

trait BladeManagerProvider {
	def newDataPartitionManager(blade: Blade): Actor = new DataPartitionManager(blade) with DataPartitionManagerProvider
	def newAggregateIndex(key: IndexKey, blade: Blade): Actor = new AggregateIndex(key, blade)
}