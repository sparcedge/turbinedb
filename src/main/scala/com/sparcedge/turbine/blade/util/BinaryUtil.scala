package com.sparcedge.turbine.blade.util

import scala.collection.mutable
import com.sparcedge.turbine.blade.event.{Event,LazyEvent}

/*
	All operations assume BigIndian byte order
*/
object BinaryUtil {
	
	def shortToBytes(num: Short): Array[Byte] = {
		val bytes = new Array[Byte](2)
		bytes(0) = (num >> 8).asInstanceOf[Byte]
		bytes(1) = (num).asInstanceOf[Byte]
		bytes
	}

	def intToBytes(num: Int): Array[Byte] = {
		val bytes = new Array[Byte](4)
		bytes(0) = (num >> 24).asInstanceOf[Byte]
		bytes(1) = (num >> 16).asInstanceOf[Byte]
		bytes(2) = (num >> 8).asInstanceOf[Byte]
		bytes(3) = (num).asInstanceOf[Byte]
		bytes
	}

	def longToBytes(num: Long): Array[Byte] = {
		val bytes = new Array[Byte](8)
		bytes(0) = (num >> 56).asInstanceOf[Byte]
		bytes(1) = (num >> 48).asInstanceOf[Byte]
		bytes(2) = (num >> 40).asInstanceOf[Byte]
		bytes(3) = (num >> 32).asInstanceOf[Byte]
		bytes(4) = (num >> 24).asInstanceOf[Byte]
		bytes(5) = (num >> 16).asInstanceOf[Byte]
		bytes(6) = (num >> 8).asInstanceOf[Byte]
		bytes(7) = (num).asInstanceOf[Byte]
		bytes
	}

	def doubleToBytes(num: Double): Array[Byte] = {
		longToBytes(java.lang.Double.doubleToLongBits(num))
	}

	def bytesToShort(bytes: Array[Byte]): Short = {
		((bytes(0) << 8)
		+ (bytes(1) & 0xff)).asInstanceOf[Short]
	}

	def bytesToInt(bytes: Array[Byte]): Int = {
		((bytes(0) << 24)
		+ (bytes(1) << 16)
		+ (bytes(2) << 8)
		+ (bytes(3) & 0xff)).asInstanceOf[Int]	
	}

	def bytesToLong(bytes: Array[Byte]): Long = {
		(((bytes(0) & 0xffL) << 56)
		+ ((bytes(1) & 0xffL) << 48)
		+ ((bytes(2) & 0xffL) << 40)
		+ ((bytes(3) & 0xffL) << 32)
		+ ((bytes(4) & 0xffL) << 24)
		+ ((bytes(5) & 0xff) << 16)
		+ ((bytes(6) & 0xff) << 8)
		+ (bytes(7) & 0xff)).asInstanceOf[Long]	
	}

	def bytesToDouble(bytes: Array[Byte]): Double = {
		java.lang.Double.longBitsToDouble(bytesToLong(bytes))
	}

	def eventToBytes(event: Event, keyIndex: EventKeyIndex): Array[Byte] = {
		val bytes = mutable.ArrayBuffer[Byte]()

		bytes ++= longToBytes(event.ts)
		val strBytes = mutable.ArrayBuffer[Byte]()
		event.strValues foreach { case (key, value) =>
			val index = keyIndex.getIndexValueAndOptionallyAdd(key)
			strBytes ++= shortToBytes(index)
			strBytes += value.size.byteValue
			strBytes ++= value.getBytes
		}
		bytes ++= shortToBytes((strBytes.size+2).shortValue)
		bytes ++= shortToBytes(event.strValues.size.shortValue)
		bytes ++= strBytes
		val dblBytes = mutable.ArrayBuffer[Byte]()
		event.dblValues foreach { case (key, value) =>
			val index = keyIndex.getIndexValueAndOptionallyAdd(key)
			dblBytes ++= shortToBytes(index)
			dblBytes ++= doubleToBytes(value)
		}
		bytes ++= shortToBytes((dblBytes.size+2).shortValue)
		bytes ++= shortToBytes(event.dblValues.size.shortValue)
		bytes ++= dblBytes

		bytes.toArray
	}

	def bytesToLazyEvent(bytes: Array[Byte], bladeMeta: BladeMetaData): LazyEvent = {
		new LazyEvent(bytes, bladeMeta.eventKeyIndex)
	}

	def bladeMetaToBytes(bladeMeta: BladeMetaData): Array[Byte] = {
		val bytes = mutable.ArrayBuffer[Byte]()
		bytes ++= longToBytes(bladeMeta.timestamp)
		bladeMeta.eventKeyIndex.indexMap.foreach { case (key, value) =>
			bytes ++= shortToBytes(key)
			bytes ++= shortToBytes(value.size.toShort)
			bytes ++= value.getBytes
		}
		bytes.toArray
	}

	def bytesToBladeMeta(bytes: Array[Byte]): BladeMetaData = {
		val timestamp = bytesToLong(slice(bytes, 0,8))
		val indexMap = mutable.Map[Short, String]()
		var curr = 8
		while(curr < bytes.size) {
			val key = bytesToShort(slice(bytes, curr, curr+2))
			curr += 2
			val vLength = bytesToShort(slice(bytes, curr, curr+2))
			curr +=2
			val value = getStringValue(bytes, curr, vLength)
			curr += vLength
			indexMap(key) = value
		}
		val keyIndex = new EventKeyIndex(indexMap)
		new BladeMetaData(timestamp, keyIndex)
	}

	def slice(bytes: Array[Byte], start: Int, until: Int): Array[Byte] = {
		val res = new Array[Byte](until - start)
		var cnt = start
		while(cnt < until) {
			res(cnt - start) = bytes(cnt)
			cnt += 1
		}
		res
	}

	def join(arr1: Array[Byte], arr2: Array[Byte]): Array[Byte] = {
		val res = new Array[Byte](arr1.length + arr2.length)
		var cnt = 0
		while(cnt < arr1.length) {
			res(cnt) = arr1(cnt)
			cnt += 1
		}
		val prev = cnt
		cnt = 0
		while(cnt < arr2.length){
			res(prev+cnt) = arr2(cnt)
			cnt += 1
		}
		res
	}

	def getStringValue(bytes: Array[Byte], pos: Int, length: Int): String = {
		new String(slice(bytes, pos, pos + length))
	}

}