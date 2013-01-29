package com.sparcedge.turbine.blade.data

import akka.actor.{Actor,ActorRef,Stash}
import com.sparcedge.turbine.blade.util.WrappedTreeMap
import com.sparcedge.turbine.blade.event.Event
import com.sparcedge.turbine.blade.query._
import java.util.HashMap
import scala.collection.mutable
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
		case event: Event =>
			updateIndex(event)
		case IndexRequest() =>
			sender ! IndexResponse(index)
		case _ =>
	}

	def updateIndex(event: Event) {
		index.update(event)
	}
}

class Index(indexKey: IndexKey, val indexManager: ActorRef, val blade: Blade) {
	val index = new WrappedTreeMap[String,ReducedResult]()
	val fullGroupings = aggregateGrouping :: indexKey.groupings.toList

	def update(event: Event) {
		if(eventMatchesAllCriteria(event, indexKey.matches)) {
			updateUnchecked(event)
		}
	}

	def updateUnchecked(event: Event) {
		val grpStr = createGroupStringForEvent(event, fullGroupings)
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(event)
	}

	def updateUnchecked(event: Event, grpStr: String) {
		val reducer = index.getOrElseUpdate(grpStr, indexKey.reducer.createReducedResult)
		reducer(event)	
	}
}