package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,Stash}
import java.util.HashMap
import scala.collection.mutable

import com.sparcedge.turbine.util.WrappedTreeMap
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.{Blade}
import QueryUtil._

object AggregateIndex {
	case class AllClear()
	case class UpdateIndex(event: Event)
	case class IndexRequest()
	case class IndexResponse(index: Index)
	case class PopulatedIndex(index: Index)
}

import AggregateIndex._
import DataPartitionManager._

class AggregateIndex(indexKey: IndexKey, blade: Blade) extends Actor with Stash {

	var cnt = 0
	var index: Index = null

	override def receive = uninitializedReceive

  	def uninitializedReceive: Receive = {
		case IndexRequest() =>
			stash()
		case PopulatedIndex(populatedIndex) =>
			index = populatedIndex
			unstashAll()
			context.become(initializedReceive)
		case _ =>
  	}

	def initializedReceive: Receive = {
		case UpdateIndex(event) =>
			updateIndex(event)
		case IndexRequest() =>
			sender ! IndexResponse(index)
		case _ =>
	}

	def updateIndex(event: Event) {
		index.update(event)
	}
}

class Index(val indexKey: IndexKey, val indexManager: ActorRef, val blade: Blade) {
	val index = new WrappedTreeMap[String,ReducedResult]()
	val groupings = indexKey.groupings

	def update(event: Event) {
		if(eventMatchesAllCriteria(event, indexKey.matches)) {
			updateUnchecked(event)
		}
	}

	def updateUnchecked(event: Event) {
		val grpStr = createDataGroupString(event, blade, groupings)
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(event)
	}

	def updateUnchecked(event: Event, grpStr: String) {
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(event)	
	}

	def updateUnchecked(value: Double, grpStr: String) {
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(value)	
	}

	def updateUnchecked(value: String, grpStr: String) {
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(value)	
	}

	def updateUnchecked(value: Long, grpStr: String) {
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(value)	
	}
}