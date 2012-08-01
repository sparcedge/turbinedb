package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import com.mongodb.casbah.query.Imports._

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