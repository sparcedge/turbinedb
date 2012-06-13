package com.sparcedge.turbine.blade.query

import scala.collection.mutable
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor

object EventCache {
	def apply(eventCursor: MongoCursor, periodStart: Long, periodEnd: Long, includedFields: Set[String], blade: Blade): EventCache = {
		val events = mutable.ListBuffer[Event]()
		val partitionManager = PartitionManager()
		var newestTimestamp = 0L
		eventCursor foreach { event =>
			events += Event(event, partitionManager)
			val its = mongoObj("its") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
			}
			if(its > newestTimestamp) {
				newestTimestamp = its
			}
		}
		new EventCache(events, periodStart, periodEnd, includedFields, newestTimestamp, blade)
	}
}

class EventCache(events: mutable.ListBuffer[Event], periodStart: Long, periodEnd: Long, val includedFields: Set[String], var newestTimestamp: Long, val blade: Blade) {

	def addEventsToCache(eventCursor: MongoCursor) {
		val partitionManager = PartitionManager()
		eventCursor foreach { event =>
			events += Event(event, partitionManager)
			val its = mongoObj("its") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
			}
			if(its > newestTimestamp) {
				newestTimestamp = its
			}
		}
	}

	def applyQuery(query: TurbineAnalyticsQuery): Iterable[Any] = {
		var timeLimitedEvents = limitEventsProcessed(query.query.range.start, query.query.range.end)
		var matchedEvents = applyMatches(timeLimitedEvents, query.query.matches)
		
		val reducers = query.query.reduce match {
			case Some(reduce) =>
				reduce.reducerList
			case None =>
				List[Reducer]()
		}

		if(reducers.isEmpty) {
			matchedEvents = matchedEvents.take(1000)
		}

		val groupedEvents = applyGroupingsAndReducers(query.query.groupings, reducers, matchedEvents)
		return groupedEvents
	}

	def limitEventsProcessed(start: Long, end: Option[Long]): Iterable[Event] = {
		if(start > periodStart || (end != None && end.get < periodEnd)) {
			events.filter(event => event.ts > start && event.ts < end.get)
		} else {
			events
		}
	}

	def applyMatches(timeLimitedEvents: Iterable[Event], matchesOpt: Iterable[Match]): Iterable[Event] = {
		matchesOpt match {
			case Nil =>
				timeLimitedEvents
			case matches =>
				timeLimitedEvents filter { event =>
					matches forall { matchCriteria =>
						matchCriteria(event)
					}
				}
		}
	}

	def applyGroupingsAndReducers(groupings: Iterable[Grouping], reducers: Iterable[Reducer], events: Iterable[Event]): Iterable[Any] = {
		groupings match {
			case Nil =>
				val (reducedValues,meta) = applyReducers(reducers,events)
				val reduceMeta = meta.getOrElse(Map[String,Any]()) + ("count" -> events.size)
				List[Any](DataGroup(None, reducedValues, reduceMeta))
			case grouping :: tail =>
				events groupBy grouping.groupFunction filterKeys (_ != null) map {case (key,value) => 
					DataGroup(Some(key), applyGroupingsAndReducers(tail,reducers,value), Map[String,Any]("count" -> value.size))
				}
		}
	}

	def applyReducers(reducers: Iterable[Reducer], events: Iterable[Event]): (Iterable[Any],Option[Map[String,Any]]) = {
		if(reducers.isEmpty) {
			(events.map(_.toMap),None)
		} else {
			val (reducedValues: Iterable[(Any,Any)],meta: Iterable[(String,Any)]) = reducers.map(_.reduceFunction(events)).unzip
			(List[Any](reducedValues.toMap),Some(meta.toMap))
		}
	}

	def includesAllFields(fields: Set[String]): Boolean = {
		fields.subsetOf(includedFields)
	}

	def getNotIncludedFields(fields: Set[String]): Set[String] = {
		includedFields.diff(fields)
	}
}

case class DataGroup (
	group: Option[Any] = None,
	data: Iterable[Any],
	meta: Map[String,Any]
)

object PartitionManager {
	def apply(): PartitionManager = {
		return new PartitionManager()
	}
}

class PartitionManager {
	val keyMaps = mutable.Map[String, mutable.Map[String,(Int,Int)]]()

	// String.intern() provides very poor porformance
	def partitionData(dataList: List[(String,Any)]): (Map[String,(Int,Int)],Array[Any],Array[Double]) = {
		val oarr = mutable.ListBuffer[Any]()
		val darr = mutable.ListBuffer[Double]()
		val mapKey = dataList.map(_._1).mkString
		val hasKeyMap = keyMaps.contains(mapKey)
		var keyMap = if (hasKeyMap) { keyMaps(mapKey) } else { mutable.Map[String,(Int,Int)]() }
		var ocount = 0
		var dcount = 0
		dataList.foreach { case (key,value) =>
			value match {
				case x: java.lang.Long =>
					darr += x.toDouble
					if(!hasKeyMap) {
						keyMap(key) = (1 -> dcount)
					}
					dcount += 1
				case x: java.lang.Integer =>
					darr += x.toDouble
					if(!hasKeyMap) {
						keyMap(key) = (1 -> dcount)
					}
					dcount += 1
				case x: java.lang.Double =>
					darr += x.toDouble
					if(!hasKeyMap) {
						keyMap(key) = (1 -> dcount)
					}
					dcount += 1
				case x: String =>
					oarr += x.intern
					if(!hasKeyMap) {
						keyMap(key) = (0 -> ocount)
					}
					ocount += 1
				case x =>
					oarr += x
					if(!hasKeyMap) {
						keyMap(key) = (0 -> ocount)
					}
					ocount += 1
			}
		}

		if(!hasKeyMap) {
			keyMaps(mapKey) = keyMap
		}

		return (keyMap.toMap, oarr.toArray, darr.toArray)
	}

	def partitionData(mongoObj: DBObject, resource: String): (Map[String,(Int,Int)],Array[Any],Array[Double]) = {
		val dataList = mongoObj("dat").asInstanceOf[DBObject].toList.+:("resource" -> resource).sortBy(_._1)
		partitionData(dataList)
	}

	def mergeData(mongoObj: DBObject, event: Event, keyMap: Map[String,(Int,Int)]): (Map[String,(Int,Int)],Array[Any],Array[Double]) = {
		val oarr = mutable.ListBuffer[Any]()
		val darr = mutable.ListBuffer[Double]()
		val dataList = mongoObj("dat").asInstanceOf[DBObject].toList ++ keyMap.map { case (x,y) => (x -> event(x).get) }
		partitionData(dataList)
	}
}

case class Event (
	ts: Long,
	var odat: Array[Any],
	var ddat: Array[Double],
	var keyMap: Map[String,(Int,Int)]
) {
	def apply(segment: String): Option[Any] = {
		val key = keyMap.get(segment)
		key match {
			case Some((arr,index)) =>
				arr match {
				case 0 =>
					Some(odat(index))
				case 1 =>
					Some(ddat(index))
			}
			case None =>
				None
		}
	}

	def toMap(): Map[String,Any] = {
		keyMap map { case (segment, (arr,index)) =>
			arr match {
				case 0 =>
					(segment -> odat(index))
				case 1 =>
					(segment -> ddat(index))
			}
		}
	}

	def updateEventData(mongoObj: DBObject, pManager: PartitionManager) {
		val (newKeyMap,newOdat,newDdat) = pManager.mergeData(mongoObj, this, keyMap)
		keyMap = newKeyMap
		odat = newOdat
		ddat = newDdat
	}
}

object Event {
	def apply(mongoObj: DBObject, pManager: PartitionManager): Event = {
		
		val (keyMap,odat,ddat) = pManager.partitionData(mongoObj, mongoObj("r").toString)

		new Event (
			mongoObj("ts") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
			},
			odat,
			ddat,
			keyMap
		)
	}
}