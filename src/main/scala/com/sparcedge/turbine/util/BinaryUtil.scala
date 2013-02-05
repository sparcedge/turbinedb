package com.sparcedge.turbine.util

import scala.collection.mutable

/*
	All operations assume BigIndian byte order
*/
object BinaryUtil {
	
	def bytes(num: Short): Array[Byte] = {
		val bytes = new Array[Byte](2)
		bytes(0) = (num >> 8).asInstanceOf[Byte]
		bytes(1) = (num).asInstanceOf[Byte]
		bytes
	}

	def bytes(num: Int): Array[Byte] = {
		val bytes = new Array[Byte](4)
		bytes(0) = (num >> 24).asInstanceOf[Byte]
		bytes(1) = (num >> 16).asInstanceOf[Byte]
		bytes(2) = (num >> 8).asInstanceOf[Byte]
		bytes(3) = (num).asInstanceOf[Byte]
		bytes
	}

	def bytes(num: Long): Array[Byte] = {
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

	def bytes(num: Double): Array[Byte] = {
		bytes(java.lang.Double.doubleToLongBits(num))
	}

	def toShort(bytes: Array[Byte]): Short = {
		((bytes(0) << 8)
		+ (bytes(1) & 0xff)).asInstanceOf[Short]
	}

	def toInt(bytes: Array[Byte]): Int = {
		((bytes(0) << 24)
		+ (bytes(1) << 16)
		+ (bytes(2) << 8)
		+ (bytes(3) & 0xff)).asInstanceOf[Int]	
	}

	def toLong(bytes: Array[Byte]): Long = {
		(((bytes(0) & 0xffL) << 56)
		+ ((bytes(1) & 0xffL) << 48)
		+ ((bytes(2) & 0xffL) << 40)
		+ ((bytes(3) & 0xffL) << 32)
		+ ((bytes(4) & 0xffL) << 24)
		+ ((bytes(5) & 0xff) << 16)
		+ ((bytes(6) & 0xff) << 8)
		+ (bytes(7) & 0xff)).asInstanceOf[Long]	
	}

	def toDouble(bytes: Array[Byte]): Double = {
		java.lang.Double.longBitsToDouble(toLong(bytes))
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

	def transfer(from: Array[Byte], to: Array[Byte], startFrom: Int, startTo: Int, count: Int) = {
		var cnt = 0
		var currFrom = startFrom
		var currTo = startTo
		while(cnt < count) {
			to(currTo) = from(currFrom)
			currFrom += 1
			currTo += 1
			cnt += 1
		}
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

	def toStringValue(bytes: Array[Byte], pos: Int, length: Int): String = {
		new String(slice(bytes, pos, pos + length))
	}

}