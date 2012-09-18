package com.sparcedge.turbine.blade.event

import scala.collection.mutable
import com.sparcedge.turbine.blade.util.BinaryUtil

class LazyEvent(bytes: Array[Byte]) extends Event {

	lazy val ts = BinaryUtil.bytesToLong(slice(bytes, 0, 8))
	lazy val strValues = createSValueMap(sValueBytes)
	lazy val dblValues = createDValueMap(dValueBytes)

	private lazy val sValueStart = 10
	private lazy val dValueStart = sValueStart + sValueSize + 2

	private lazy val sValueSize = BinaryUtil.bytesToShort(slice(bytes, 8, 10))
	private lazy val dValueSize = BinaryUtil.bytesToShort(slice(bytes, sValueStart+sValueSize, sValueStart+sValueSize+2))

	private lazy val sValueBytes = slice(bytes, sValueStart, sValueStart+sValueSize)
	private lazy val dValueBytes = slice(bytes, dValueStart, dValueStart+dValueSize)

	private def createSValueMap(bytes: Array[Byte]): mutable.Map[String,String] = {
		val sValues = mutable.Map[String,String]()
		val size = BinaryUtil.bytesToShort(slice(bytes, 0, 2))

		var cnt = 0
		var curr = 2
		while(cnt < size) {
			val kLength = bytes(curr)
			val key = getStringValue(bytes, curr+1, kLength) 
			curr += kLength + 1
			val vLength = bytes(curr)
			val value = getStringValue(bytes, curr+1, vLength)
			curr += vLength + 1
			sValues += (key -> value)
			cnt += 1
		}

		sValues
	}

	private def createDValueMap(bytes: Array[Byte]): mutable.Map[String,Double] = {
		val dValues = mutable.Map[String,Double]()
		val size = BinaryUtil.bytesToShort(slice(bytes, 0, 2))

		var cnt = 0
		var curr = 2
		while(cnt < size) {
			val kLength = bytes(curr)
			val key = getStringValue(bytes, curr+1, kLength) 
			curr += kLength + 1
			val value = BinaryUtil.bytesToDouble(slice(bytes, curr, curr + 8))
			curr += 8
			dValues += (key -> value)
			cnt += 1
		}

		dValues
	}

	private def getStringValue(bytes: Array[Byte], pos: Int, length: Int): String = {
		new String(slice(bytes, pos, pos + length))
	}

	private def slice(bytes: Array[Byte], start: Int, until: Int): Array[Byte] = {
		val res = new Array[Byte](until - start)
		var cnt = start
		while(cnt < until) {
			res(cnt - start) = bytes(cnt)
			cnt += 1
		}
		res
	}

}