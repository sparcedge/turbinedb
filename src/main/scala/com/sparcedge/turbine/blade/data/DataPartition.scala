package com.sparcedge.turbine.blade.data

import java.io._
import scala.collection.mutable
import akka.actor.ActorRef

import com.sparcedge.turbine.blade.event.{Event,ConcreteEvent}
import com.sparcedge.turbine.blade.util.{DiskUtil,Timer,CustomByteBuffer}
import com.sparcedge.turbine.blade.query.{Blade,Match,Grouping}

import com.sparcedge.turbine.blade.util.DiskUtil._
import com.sparcedge.turbine.blade.util.BinaryUtil._
import com.sparcedge.turbine.blade.data.QueryUtil._
import AggregateIndex._

//TODO: Replace cache with data
class DataPartition(val blade: Blade) {

	ensureCacheDirectoryExists(blade)

	val partitionDirectory = getDirectoryForBlade(blade)
	var dataSegments = retrieveAllExistingSegments(blade).toList
	var eventCount = retrieveExistingEventCount(blade)
	var newestTimestamp = retrieveLatestInternalTimestamp(blade)
	val lngArr = new Array[Byte](8)

	def writeEvent(event: Event) {
		val eventSegments = getEventSegments(event)

		if(event.its > newestTimestamp) 
			newestTimestamp = event.its
		
		if(!containsAllSegments(eventSegments)) 
			addNewSegments(eventSegments)
		
		val segmentOutStreamMap = createSegmentOutputStreamMap(eventSegments)
		writeEventToSegmentFiles(event, segmentOutStreamMap)
		eventCount += 1
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

	def addNewSegments(segments: Iterable[String]) = {
		segments.filterNot(dataSegments.contains).foreach { segmentToAdd =>
			ensureCacheSegmentFileExists(blade, segmentToAdd)
			dataSegments = segmentToAdd :: dataSegments
		}
	}

	def containsAllSegments(segments: Iterable[String]): Boolean = {
		segments.forall(dataSegments.contains(_))
	}

	def getEventSegments(event: Event): Iterable[String] = {
		(event.dblValues.keySet ++ event.strValues.keySet) + "ts" + "its"
	}

	def createSegmentOutputStreamMap(segments: Iterable[String]): Map[String,BufferedOutputStream] = {
		segments.map { segment =>
			segment -> (new BufferedOutputStream (
				new FileOutputStream(getDataFileNameForBladeSegment(blade, segment), true), 128 * 100
			))
		}.toMap
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

	def populateIndexes(indexes: Iterable[Index], reqSegments: Iterable[String], optSegments: Iterable[String], matches: Iterable[Match], groupings: Iterable[Grouping]) {
		if(reqSegments.forall(dataSegments.contains(_))) {
			populateIndexes(indexes, reqSegments ++ optSegments, matches, groupings)
		}
	}

	def populateIndexes(indexes: Iterable[Index], segments: Iterable[String], matches: Iterable[Match], groupings: Iterable[Grouping]) {
		val segmentBufferList = createSegmentBufferList("ts" :: segments.toList) 
		val timer = new Timer()
		timer.start()
		var cnt = 0
		try {
			val tsBuffer = segmentBufferList.find(_._1 == "ts").get._2
			while(tsBuffer.hasRemaining) {
				val event = readEventFromBuffers(segmentBufferList)
				if(eventMatchesAllCriteria(event, matches)) {
					val fullGroupings = aggregateGrouping :: groupings.toList
					val grpStr = createGroupStringForEvent(event, fullGroupings)
					indexes.foreach(_.updateUnchecked(event, grpStr))
				}
				cnt += 1
			}
		} catch {
			case e: Exception => e.printStackTrace
		} finally {
			segmentBufferList.foreach(_._2.close())
		}
		timer.stop("Processed " + cnt + " Events")
	}

	def readEventFromBuffers(bufferList: Iterable[(String, CustomByteBuffer)]): Event = {
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()
		var timestamp = 0L

		bufferList.foreach { segBuf =>
			if(segBuf._1 == "ts") {
				segBuf._2.readBytes(lngArr, 8)
				timestamp = toLong(lngArr)
			} else {
				readSegmentToCorrectMap(segBuf._1, segBuf._2, strValues, dblValues)
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

	def createSegmentBufferList(segments: Iterable[String]): Iterable[(String, CustomByteBuffer)] = {
		segments.flatMap { segment => 
			if(dataSegments.contains(segment)) {
				Some((segment -> new CustomByteBuffer(getDataFileNameForBladeSegment(blade, segment), DEFAULT_PAGE_SIZE)))
			} else {
				None
			}
		}
	}
}