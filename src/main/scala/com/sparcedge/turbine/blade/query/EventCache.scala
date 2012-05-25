package com.sparcedge.turbine.blade.query

import scala.collection.mutable
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor

object EventCache {
	def apply(eventCursor: MongoCursor, periodStart: Long, periodEnd: Long): EventCache = {
		val events = mutable.ListBuffer[Event]()
		val partitionManager = PartitionManager()
		eventCursor foreach { event =>
			events += Event(event, partitionManager)
		}
		new EventCache(events, periodStart, periodEnd)
	}
}

class EventCache(events: Iterable[Event], periodStart: Long, periodEnd: Long) {

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
			events.filter(event => event.ts > periodStart && event.ts < periodEnd)
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
	def partitionData(mongoObj: DBObject, resource: String): (Map[String,(Int,Int)],Array[Any],Array[Double]) = {
		val oarr = mutable.ListBuffer[Any]()
		val darr = mutable.ListBuffer[Double]()
		val dataList = mongoObj("dat").asInstanceOf[DBObject].toList.+:("resource" -> resource).sortBy(_._1)
		val mapKey = dataList.map(_._1).mkString
		val hasKeyMap = keyMaps.contains(mapKey)
		var keyMap = if (hasKeyMap) { keyMaps(mapKey) } else { mutable.Map[String,(Int,Int)]() }
		var ocount = 0
		var dcount = 0
		dataList.foreach { case (key,value) =>
			value match {
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
}

case class Event (
	ts: Long,
	odat: Array[Any],
	ddat: Array[Double],
	keyMap: Map[String,(Int,Int)]
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
}

object Event {
	def apply(mongoObj: DBObject, pManager: PartitionManager): Event = {
		
		val (keyMap,odat,ddat) = pManager.partitionData(mongoObj, mongoObj("r").toString)

		new Event (
			mongoObj("ts").asInstanceOf[Double].toLong,
			odat,
			ddat,
			keyMap
		)
	}
}