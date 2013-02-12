package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,Props}
import scala.collection.mutable
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.event.Event
import scala.util.Random

object BladeManager {

	case class IndexesRequest(query: TurbineQuery)

	case class IndexesResponse(indexes: Iterable[ActorRef])

	case class AddEvent(event: Event)
}

import BladeManager._
import DataPartitionManager._

class BladeManager(blade: Blade) extends Actor {

	val partitionManager = context.actorOf (
		Props(new DataPartitionManager(blade)).withDispatcher("com.sparcedge.turbinedb.data-partition-dispatcher"), 
		s"${blade.toString}-partition-manager"
	)
	val indexMap = mutable.Map[IndexKey,ActorRef]()

	def receive = {
		case IndexesRequest(query) =>
			val newIndexes = mutable.ListBuffer[Index]()

			val indexes = retrieveIndexKeysFromQuery(query) map { key => 
				indexMap.getOrElseUpdate(key, createAggregateIndex(key, newIndexes))
			}
			if(newIndexes.size > 0) {
				beginIndexPopulation(newIndexes)
			}
			sender ! IndexesResponse(indexes)
		case AddEvent(event) =>
			partitionManager ! WriteEvent(event)
	}

	def retrieveIndexKeysFromQuery(query: TurbineQuery): Iterable[IndexKey] = {
		val reducers = query.reduce match {
			case Some(reduce) => reduce.reducerList
			case None => List[Reducer]()
		}
		reducers.map(query.createAggregateIndexKey(_))
	}

	def createAggregateIndex(key: IndexKey, newIndexes: mutable.ListBuffer[Index]): ActorRef = {
		val indexActor = context.actorOf(Props(new AggregateIndex(key, blade)).withDispatcher("com.sparcedge.turbinedb.agg-index-dispatcher"), key.id)
		val index = new Index(key, indexActor, blade)
		index +=: newIndexes
		indexActor
	}

	def beginIndexPopulation(indexes: Iterable[Index]) {
		partitionManager ! PopulateIndexesRequest(indexes)
	}
}

case class IndexKey (reducer: Reducer, matches: Iterable[Match], groupings: Iterable[Grouping]) {
	val id = s"${reducer.reducer}.${reducer.segment}.${matches.size}.${groupings.size}.${Random.nextLong}"
}