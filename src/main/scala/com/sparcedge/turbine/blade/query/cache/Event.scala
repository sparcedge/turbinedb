package com.sparcedge.turbine.blade.query.cache

import com.mongodb.casbah.query.Imports._

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