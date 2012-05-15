package com.sparcedge.turbine.blade.query

import scala.collection.mutable
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor

object EventCache {
	def apply(eventCursor: MongoCursor): EventCache = {
		val events = mutable.ListBuffer[Event]()
		val partitionManager = PartitionManager()
		eventCursor foreach { event =>
			events += Event(event, partitionManager)
		}
		new EventCache(events)
	}
}

class EventCache(events: Iterable[Event]) {

	def applyQuery(query: TurbineAnalyticsQuery): Iterable[Any] = {
		println(events.size)
		val matchedEvents = applyMatches(query.query.matches)
		return matchedEvents
	}

	def applyMatches(matchesOpt: Iterable[Match]): Iterable[Event] = {
		matchesOpt match {
			case Nil =>
				events
			case matches =>
				events filter { event =>
					matches forall { matchCriteria =>
						matchCriteria(event)
					}
				}
		}
	}
}

object PartitionManager {
	def apply(): PartitionManager = {
		return new PartitionManager()
	}
}

class PartitionManager {
	val keyMaps = mutable.Map[String, mutable.Map[String,(Int,Int)]]()

	def partitionData(mongoObj: DBObject, resource: String): (Map[String,(Int,Int)],Array[Any],Array[Double]) = {
		val oarr = mutable.ListBuffer[Any]()
		val darr = mutable.ListBuffer[Double]()
		val dataList = mongoObj("dat").asInstanceOf[DBObject].toList.+:("resource" -> resource).sortBy(_._1)
		val mapKey = dataList.map(_._1).mkString
		val hasKeyMap = keyMaps.contains(mapKey)
		var keyMap = if (hasKeyMap) { keyMaps(mapKey) } else { mutable.Map[String,(Int,Int)]() }
		var count = 0
		dataList.foreach { case (key,value) =>
			value match {
				case x: java.lang.Double =>
					darr += x.toDouble
					if(!hasKeyMap) {
						keyMap(key) = (1 -> count)
					}
				case x: String =>
					oarr += x.intern
					if(!hasKeyMap) {
						keyMap(key) = (0 -> count)
					}
				case x =>
					oarr += x
					if(!hasKeyMap) {
						keyMap(key) = (0 -> count)
					}
			}
			count += 1
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
	def apply(key: String): Any = {
		val (arr,index) = keyMap(key)
		arr match {
			case 0 =>
				odat(index)
			case 1 =>
				ddat(index)
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