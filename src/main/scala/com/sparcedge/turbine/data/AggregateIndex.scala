package com.sparcedge.turbine.data

import akka.actor.{Actor,ActorRef,Stash,ActorLogging}
import java.util.HashMap
import scala.collection.mutable

import com.sparcedge.turbine.util.WrappedTreeMap
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.query.pipeline._
import com.sparcedge.turbine.{Blade}
import QueryUtil._

object AggregateIndex {
	case class AllClear()
	case class UpdateIndex(event: Event)
	case class IndexRequest()
	case class IndexResponse(index: Index)
	case class PopulatedIndex(index: Index)
}

import DataTypes._
import AggregateIndex._
import DataPartitionManager._

// TODO: Rename to IndexManager
class AggregateIndex(indexKey: IndexKey, blade: Blade) extends Actor with ActorLogging with Stash {

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
			log.debug("Index population complete ({})", indexKey.id)
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

// TODO: Make sure can extend/match/group/reduce from single event!
class Index(val indexKey: IndexKey, val indexManager: ActorRef, val blade: Blade) {
	val index = new WrappedTreeMap[String,ReducedResult]()
	val groupings = indexKey.groupings
	val extenders = indexKey.extenders
	var segmentPlaceholder = SegmentValueHolder(indexKey.reducer.segment)
	val indexGrouping = IndexGrouping("hour", blade.periodStartMS)

	def apply(placeholder: SegmentValueHolder) {
		if(placeholder.segment == indexKey.reducer.segment) {
			segmentPlaceholder = placeholder
		}
	}

	def evaluate(grpStr: String) = (segmentPlaceholder.getType) match {
        case NUMBER => updateUnchecked(segmentPlaceholder.getDouble, grpStr)
        case STRING => updateUnchecked(segmentPlaceholder.getString, grpStr)
        case TIMESTAMP => updateUnchecked(segmentPlaceholder.getTimestamp, grpStr)
        case _ =>
    }

	def update(event: Event) {
		val extEvent = if (extenders.size > 0) extendEvent(event, extenders) else event

		if(eventMatchesAllCriteria(event, indexKey.matches)) {
			updateUnchecked(event)
		}
	}

	def updateUnchecked(event: Event) {
		val grpStr = createGroupString(event, indexGrouping +: groupings)
		val reducer = getOrUpdate(grpStr)
		reducer(event)
	}

	def updateUnchecked(event: Event, grpStr: String) {
		val reducer = getOrUpdate(grpStr)
		reducer(event)	
	}

	def updateUnchecked(value: Double, grpStr: String) {
		val reducer = getOrUpdate(grpStr)
		reducer(value)	
	}

	def updateUnchecked(value: String, grpStr: String) {
		val reducer = getOrUpdate(grpStr)
		reducer(value)	
	}

	def updateUnchecked(value: Long, grpStr: String) {
		val reducer = getOrUpdate(grpStr)
		reducer(value)	
	}

	def getOrUpdate(key: String): ReducedResult = {
		var reducedResult = index.getUnsafe(key)
		if(reducedResult == null) {
			reducedResult = indexKey.reducer.createReducedResult
			index.update(key, reducedResult)
		}
		reducedResult
	}
}