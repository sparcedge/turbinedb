package com.sparcedge.turbine.blade.data

import akka.actor.{Actor,ActorRef,Props}
import scala.collection.mutable
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.event.Event
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
			val newKeys = mutable.ListBuffer[IndexKey]()

			val indexes = retrieveIndexKeysFromQuery(query) map { key => 
				indexMap.getOrElseUpdate(key, createAggregateIndex(key, newIndexes, newKeys))
			}
			if(newIndexes.size > 0) {
				beginIndexPopulation(newIndexes, newKeys)
			}
			sender ! IndexesResponse(indexes)
		case AddEvent(event) =>
			partitionManager ! WriteEvent(event)
	}

	def retrieveIndexKeysFromQuery(query: TurbineQuery): Iterable[IndexKey] = {
		val reducers = query.query.reduce match {
			case Some(reduce) => reduce.reducerList
			case None => List[Reducer]()
		}
		reducers.map(query.query.createAggregateIndexKey(_))
	}

	def createAggregateIndex(key: IndexKey, newIndexes: mutable.ListBuffer[Index], newKeys: mutable.ListBuffer[IndexKey]): ActorRef = {
		val indexActor = context.actorOf(Props(new AggregateIndex(key, blade)).withDispatcher("com.sparcedge.turbinedb.agg-index-dispatcher"), key.id)
		val index = new Index(key, indexActor, blade)
		index +=: newIndexes
		key +=: newKeys
		indexActor
	}

	def beginIndexPopulation(indexes: Iterable[Index], keys: Iterable[IndexKey]) {
		val reqSegments = retrieveRequiredSegments(keys)
		val optSegments = retrieveOptionalSegments(keys)
		val matches = keys.head.matches
		val groupings = keys.head.groupings
		partitionManager ! PopulateIndexesRequest(indexes, reqSegments, optSegments, matches, groupings)
	}

	def retrieveRequiredSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		keys.head.matches.map(_.segment) ++: keys.head.groupings.flatMap(_.segment)
	}

	def retrieveOptionalSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		keys.map(_.reducer.segment)
	}
}

case class IndexKey (reducer: Reducer, matches: Iterable[Match], groupings: Iterable[Grouping]) {
	val id = s"${reducer.reducer}.${reducer.segment}.${matches.size}.${groupings.size}.${Random.nextLong}"
}