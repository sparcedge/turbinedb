package com.sparcedge.turbine.blade.util

import java.io.RandomAccessFile

class CustomByteBuffer(fileName: String, size: Int) {
	private val file = new RandomAccessFile(fileName, "r")
	private var buffer = new Array[Byte](size)
	private var currIndex = 0
	private var bSize = 0

	bSize = file.read(buffer)

	def getBytes(num: Int): Array[Byte] = {
		if(currIndex + num < bSize) {
			val res = BinaryUtil.slice(buffer, currIndex, currIndex + num)
			currIndex = currIndex + num
			res
		} else {
			var bytesLeft = num
			var res = new Array[Byte](0)
			while(bytesLeft > 0) {
				val overSize = (bytesLeft > bSize - currIndex)
				val endIndex = if (overSize) bSize else currIndex + bytesLeft
				val temp = BinaryUtil.slice(buffer, currIndex, endIndex)
				res = BinaryUtil.join(res, temp)
				bytesLeft -= endIndex - currIndex
				currIndex = endIndex
				if(overSize) {
					bSize = file.read(buffer)
					currIndex = 0
				}
			}
			res
		}
	}

	def hasRemaining(): Boolean = {
		if(bSize > 0) {
			true
		} else {
			bSize = file.read(buffer)
			bSize > 0
		}
	}
 
	def close() {
		file.close()
	}
}