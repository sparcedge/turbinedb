package com.sparcedge.turbine.blade.cache

import java.io._
import scala.collection.mutable
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor

import com.sparcedge.turbine.blade.event.{Event,LazyEvent,ConcreteEvent}
import com.sparcedge.turbine.blade.util._
import com.sparcedge.turbine.blade.query.Blade

class DiskCache(val blade: Blade) {

	DiskUtil.ensureCacheDirectoryExists(blade)
	DiskUtil.ensureMetaFileExists(blade)

	val cacheDir = DiskUtil.getDirectoryForBlade(blade)
	var metaData = DiskUtil.readBladeMetaFromDisk(blade)
	var cacheSegments = DiskUtil.retrieveAllExistingSegments(blade).toList
	var eventCount = DiskUtil.retrieveExistingEventCount(blade)

	def addEvents(cursor: MongoCursor) {
		addEventsAndExecute(cursor) { e => /* Empty Block */ }
	}

	// TODO: Potentially remove ConcreteEvent from equation (Less Overhead)
	def addEventsAndExecute(cursor: MongoCursor)(fun: (Event) => Unit) {
		val timer = new Timer
		var newestTimestamp = metaData.timestamp
		var segmentOutStreamMap = createSegmentOutputStreamMap(cacheSegments)
		val startCount = eventCount
		timer.start()
		try {
			cursor foreach { rawEvent =>
				val event = ConcreteEvent.fromRawEvent(rawEvent)
				if(!cacheContainsAllSegmentsForEvent(event)) {
					segmentOutStreamMap = addNewSegments(event, segmentOutStreamMap)
				}
				if(event.its > newestTimestamp) {
					newestTimestamp = event.its
				}
				writeEventToSegmentFiles(event, segmentOutStreamMap)
				fun(event)
				eventCount += 1
				if(eventCount % 100 == 0) {
					println("Events Imported: " + eventCount)
				}
			}
		} catch {
			case e: Exception => e.printStackTrace
		} finally {
			segmentOutStreamMap.values.foreach { outStream => outStream.flush(); outStream.close() }
		}
		timer.stop("Wrote " + (eventCount - startCount) + " Events to Cache (" + blade + ")")

		metaData = new BladeMetaData(newestTimestamp)
		DiskUtil.updateCacheMetadata(blade, metaData)
	}

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
				outStream.write(BinaryUtil.longToBytes(event.ts))
			} else {
				writeEventSegment(event, segment, outStream)
			}
		}
	}

	def writeEventSegment(event: Event, segment: String, outStream: BufferedOutputStream) {
		event.dblValues.get(segment) match {
			case Some(value) => 
				outStream.write(1.byteValue)
				outStream.write(BinaryUtil.doubleToBytes(value))
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
		timer.start()
		try {
			val tsBuffer = segmentBufferMap("ts")
			while(tsBuffer.hasRemaining) {
				processFun(readEventFromBuffers(segmentBufferMap))
				cnt += 1
			}
		} catch {
			case e: Exception => e.printStackTrace
		} finally {
			segmentBufferMap.values.foreach(_.close())
		}

		timer.stop("Processed " + cnt + " Events")
	}

	def readEventFromBuffers(bufferMap: Map[String, CustomByteBuffer]): Event = {
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()
		var timestamp = 0L

		bufferMap.foreach { case (segment, buffer) =>
			if(segment == "ts") {
				timestamp = BinaryUtil.bytesToLong(bufferMap("ts").getBytes(8))
			} else {
				readSegmentToCorrectMap(segment, buffer, strValues, dblValues)
			}
		}

		new ConcreteEvent(0L, timestamp, strValues, dblValues)
	}

	// TODO: Add direct read single byte to custom buffer
	def readSegmentToCorrectMap(segment: String, buffer: CustomByteBuffer, strValues: mutable.Map[String,String], dblValues: mutable.Map[String,Double]) {
		val byte = buffer.getBytes(1)(0)
		if(byte == 0) {
			// Skip -- segment does not exist for event
		} else if(byte == 1) {
			dblValues += (segment -> BinaryUtil.bytesToDouble(buffer.getBytes(8)))
		} else if(byte == 2) {
			val len = buffer.getBytes(1)(0)
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