package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,Props}
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

class BladeManager(blade: Blade) extends Actor {

	val partitionManager = context.actorOf (
		Props(new DataPartitionManager(blade)).withDispatcher("com.sparcedge.turbinedb.data-partition-dispatcher"), 
		s"${blade.toString}-partition-manager"
	)

	val indexMap = mutable.Map[String,ActorRef]()

	def receive = {
		case IndexesRequest(query) =>
			println("Receieved Indexes Request")
			val newIndexes = mutable.ListBuffer[Index]()

			val indexes = retrieveIndexKeysFromQuery(query) map { key => 
				indexMap.getOrElseUpdate(key.id, createAggregateIndex(key, newIndexes))
			}
			println("Got / Created Indexes")
			if(newIndexes.size > 0) {
				beginIndexPopulation(newIndexes)
			}
			sender ! IndexesResponse(indexes)
			println("Responded With Indexes")
		case AddEvent(id, event) =>
			partitionManager ! WriteEvent(id, event)
			// TODO: Efficiency
			indexMap.values.foreach(_ ! UpdateIndex(event))
		case _ =>
	}

	def retrieveIndexKeysFromQuery(query: TurbineQuery): Iterable[IndexKey] = {
		query.reducers.map(query.createAggregateIndexKey(_))
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

case class IndexKey (reducer: CoreReducer, matches: Iterable[Match], groupings: Iterable[Grouping]) {
	val uniqueMatchStr = matches.map(_.uniqueId).toList.sorted.mkString(".")
	val uniqueGroupStr = groupings.map(_.uniqueId).toList.sorted.mkString(".")
	val id = s"${reducer.reducer}.${reducer.segment}.${uniqueMatchStr}.${uniqueGroupStr}"
}









