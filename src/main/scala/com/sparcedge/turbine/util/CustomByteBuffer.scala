package com.sparcedge.turbine.util

import java.io.RandomAccessFile
import BinaryUtil._

class CustomByteBuffer(fileName: String, size: Int) {
	private val file = new RandomAccessFile(fileName, "r")
	private var buffer = new Array[Byte](size)
	private var currIndex = 0
	private var bSize = 0

	bSize = file.read(buffer)

	def readByte(): Byte = {
		if(currIndex < bSize) {
			val byte = buffer(currIndex)
			currIndex += 1
			byte
		} else {
			bSize = file.read(buffer)
			currIndex = 0
			val byte = buffer(currIndex)
			currIndex += 1
			byte
		}
	}

	def readBytes(arr: Array[Byte], num: Int): Int = {
		if(currIndex + num < bSize) {
			transfer(buffer, arr, currIndex, 0, num)
			currIndex = currIndex + num
			num
		} else {
			var bytesLeft = num
			var arrIndex = 0
			while(bytesLeft > 0) {
				val overSize = bytesLeft > (bSize - currIndex)
				val bytesToTake = if (overSize) bSize - currIndex else bytesLeft
				transfer(buffer, arr, currIndex, arrIndex, bytesToTake)
				arrIndex += bytesToTake
				bytesLeft -= bytesToTake
				currIndex += bytesToTake
				if(overSize) {
					bSize = file.read(buffer)
					currIndex = 0
					if(bSize < 0) 
						throw new Exception("Asked for more bytes than contained in file!")
				}
			}
			num
		}
	}

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
				val temp = slice(buffer, currIndex, endIndex)
				res = join(res, temp)
				bytesLeft -= endIndex - currIndex
				currIndex = endIndex
				if(overSize) {
					bSize = file.read(buffer)
					currIndex = 0
					if(bSize < 0)
						throw new Exception("Asked for more bytes than contained in file!")
				}
			}
			res
		}
	}

	def hasRemaining(): Boolean = {
		if(bSize > 0 && currIndex < bSize) {
			true
		} else {
			bSize = file.read(buffer)
			currIndex = 0
			bSize > 0
		}
	}
 
	def close() {
		file.close()
	}
}