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

	def eventToBytes(event: Event): Array[Byte] = {
		val bytes = mutable.ArrayBuffer[Byte]()

		bytes ++= BinaryUtil.longToBytes(event.ts)
		val strBytes = mutable.ArrayBuffer[Byte]()
		event.strValues foreach { case (key, value) =>
			strBytes += key.size.byteValue
			strBytes ++= key.getBytes
			strBytes += value.size.byteValue
			strBytes ++= value.getBytes
		}
		bytes ++= BinaryUtil.shortToBytes((strBytes.size+2).shortValue)
		bytes ++= BinaryUtil.shortToBytes(event.strValues.size.shortValue)
		bytes ++= strBytes
		val dblBytes = mutable.ArrayBuffer[Byte]()
		event.dblValues foreach { case (key, value) =>
			dblBytes += key.size.byteValue
			dblBytes ++= key.getBytes
			dblBytes ++= BinaryUtil.doubleToBytes(value)
		}
		bytes ++= BinaryUtil.shortToBytes((dblBytes.size+2).shortValue)
		bytes ++= BinaryUtil.shortToBytes(event.dblValues.size.shortValue)
		bytes ++= dblBytes

		bytes.toArray
	}

	def bytesToLazyEvent(bytes: Array[Byte]): LazyEvent = {
		new LazyEvent(bytes)
	}

}