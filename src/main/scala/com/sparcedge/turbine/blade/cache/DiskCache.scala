package com.sparcedge.turbine.blade.cache

import java.io._
import scala.collection.mutable

import com.sparcedge.turbine.blade.event.{Event,ConcreteEvent}
import com.sparcedge.turbine.blade.util.{DiskUtil,Timer,CustomByteBuffer}
import com.sparcedge.turbine.blade.util.BinaryUtil._
import com.sparcedge.turbine.blade.query.Blade

class DiskCache(val blade: Blade) {

	DiskUtil.ensureCacheDirectoryExists(blade)

	val cacheDir = DiskUtil.getDirectoryForBlade(blade)
	var cacheSegments = DiskUtil.retrieveAllExistingSegments(blade).toList
	var eventCount = DiskUtil.retrieveExistingEventCount(blade)
	var newestTimestamp = DiskUtil.retrieveLatestInternalTimestamp(blade)
	val lngArr = new Array[Byte](8)

	def addNewSegments(event: Event, segmentOutStreamMap: Map[String,BufferedOutputStream]): Map[String,BufferedOutputStream] = {
		var newMap = segmentOutStreamMap
		val eventSegments = event.dblValues.keys ++ event.strValues.keys
		eventSegments.filter(!cacheSegments.contains(_)).foreach { segment =>
			DiskUtil.ensureCacheSegmentFileExists(blade, segment)
			DiskUtil.padSegmentFileZeroBytes(blade, segment, eventCount)
			cacheSegments = segment :: cacheSegments
			newMap += segment -> (new BufferedOutputStream (
				new FileOutputStream(DiskUtil.getDataFileNameForBladeSegment(blade, segment), true), 128 * 100
			))
		}
		newMap
	}

	def cacheContainsAllSegmentsForEvent(event: Event): Boolean = {
		cacheContainsAllSegments(event.dblValues.keys) &&
		cacheContainsAllSegments(event.strValues.keys)
	}

	def cacheContainsAllSegments(segments: Iterable[String]): Boolean = {
		segments.forall(cacheSegments.contains(_))
	}

	def createSegmentOutputStreamMap(segments: List[String]): Map[String,BufferedOutputStream] = {
		segments.map { segment =>
			segment -> (new BufferedOutputStream (
				new FileOutputStream(DiskUtil.getDataFileNameForBladeSegment(blade, segment), true), 128 * 100
			))
		}.toMap
	}

	def writeEventToSegmentFiles(event: Event, segmentOutStreamMap: Map[String,BufferedOutputStream]) {
		segmentOutStreamMap foreach { case (segment, outStream) =>
			if(segment == "ts") {
				outStream.write(bytes(event.ts))
			} else {
				writeEventSegment(event, segment, outStream)
			}
		}
	}

	def writeEventSegment(event: Event, segment: String, outStream: BufferedOutputStream) {
		event.dblValues.get(segment) match {
			case Some(value) => 
				outStream.write(1.byteValue)
				outStream.write(bytes(value))
			case None => event.strValues.get(segment) match {
				case Some(value) =>
					outStream.write(2.byteValue)
					outStream.write(value.size.byteValue)
					outStream.write(value.getBytes)
				case None => 
					outStream.write(0.byteValue)
			}
		}
	}

	def processEvents(reqSegments: Iterable[String], optSegments: Iterable[String])(processFun: (Event) => Unit) {
		if(reqSegments.forall(cacheSegments.contains(_))) {
			processEvents(reqSegments ++ optSegments)(processFun)
		}
	}

	def processEvents(segments: Iterable[String])(processFun: (Event) => Unit) {
		val segmentBufferMap = createSegmentBufferMap("ts" :: segments.toList) 
		val timer = new Timer
		var cnt = 0
		var diskTime = 0L
		var processTime = 0L
		timer.start()
		try {
			val tsBuffer = segmentBufferMap("ts")
			while(tsBuffer.hasRemaining) {
				val ds = System.currentTimeMillis
				val event = readEventFromBuffers(segmentBufferMap)
				diskTime += (System.currentTimeMillis - ds)

				val ps = System.currentTimeMillis
				processFun(event)
				processTime += (System.currentTimeMillis - ps)
				cnt += 1
			}
		} catch {
			case e: Exception => e.printStackTrace
		} finally {
			segmentBufferMap.values.foreach(_.close())
		}

		println(s"Disk Time: ${diskTime}, Process Time: ${processTime}")

		timer.stop("Processed " + cnt + " Events")
	}

	def readEventFromBuffers(bufferMap: Map[String, CustomByteBuffer]): Event = {
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()
		var timestamp = 0L

		bufferMap.foreach { case (segment, buffer) =>
			if(segment == "ts") {
				bufferMap("ts").readBytes(lngArr, 8)
				timestamp = toLong(lngArr)
			} else {
				readSegmentToCorrectMap(segment, buffer, strValues, dblValues)
			}
		}

		new ConcreteEvent(0L, timestamp, strValues, dblValues)
	}

	def readSegmentToCorrectMap(segment: String, buffer: CustomByteBuffer, strValues: mutable.Map[String,String], dblValues: mutable.Map[String,Double]) {
		val byte = buffer.readByte
		if(byte == 0) {
			// Skip -- segment does not exist for event
		} else if(byte == 1) {
			buffer.readBytes(lngArr,8)
			dblValues += (segment -> toDouble(lngArr))
		} else if(byte == 2) {
			val len = buffer.readByte
			strValues += (segment -> new String(buffer.getBytes(len)))
		} else {
			// Unrecognized Byte! - Probably bad!
		}
	}

	def createSegmentBufferMap(segments: Iterable[String]): Map[String, CustomByteBuffer] = {
		segments.flatMap { segment => 
			if(cacheSegments.contains(segment)) {
				Some((segment -> new CustomByteBuffer(DiskUtil.getDataFileNameForBladeSegment(blade, segment), DiskUtil.DEFAULT_PAGE_SIZE)))
			} else {
				None
			}
		}.toMap
	}

}