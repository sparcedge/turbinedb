package com.sparcedge.turbine.data

import java.io._
import scala.collection.mutable
import akka.actor.ActorRef

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.{DiskUtil,Timer,CustomByteBuffer}
import com.sparcedge.turbine.query.{Blade,Match,Grouping}

import com.sparcedge.turbine.util.DiskUtil._
import com.sparcedge.turbine.util.BinaryUtil._
import com.sparcedge.turbine.data.QueryUtil._
import AggregateIndex._

class DataPartition(val blade: Blade) {

	ensureCacheDirectoryExists(blade)

	val partitionDirectory = getDirectoryForBlade(blade)
	var dataSegments = retrieveAllExistingSegments(blade).toList
	var eventCount = retrieveExistingEventCount(blade)
	val lngArr = new Array[Byte](8)

	def writeEvent(event: Event) {
		writeEvents(List[Event](event))
	}

	def writeEvents(events: Iterable[Event]) {
		val eventWriter = new EventOutputWriter(dataSegments, events)
		try {
			eventWriter.writeEvents()
			eventCount += events.size
		} finally {
			eventWriter.close()
			dataSegments = dataSegments ++ eventWriter.newSegments
		}
	}

	def populateIndexes(indexes: Iterable[Index]) {
		val keys = indexes.map(_.indexKey)
		val reqSegments = retrieveRequiredSegments(keys)
		val optSegments = retrieveOptionalSegments(keys)
		if(reqSegments.forall(dataSegments.contains(_)) && optSegments.exists(dataSegments.contains(_))) {
			populateIndexes(indexes, reqSegments ++ optSegments)
		}
	}

	def populateIndexes(indexes: Iterable[Index], segments: Iterable[String]) {
		val matches = indexes.head.indexKey.matches
		val groupings = indexes.head.indexKey.groupings
		val matchBuilder = new MatchBuilder(matches)
		val groupStrBuilder = new GroupStringBuilder(aggregateGrouping, groupings, blade)
		val indexUpdateBuilder = new IndexUpdateBuilder(indexes)
		val segmentBuffers = createSegmentBuffers(segments.toList) 
		val tsBuffer = new SegmentBuffer("ts", blade)
		val timer = new Timer()
		timer.start()
		var cnt = 0
		try {
			
			while(tsBuffer.buffer.hasRemaining) {
				readNextFromBuffers(segmentBuffers, tsBuffer, matchBuilder, groupStrBuilder, indexUpdateBuilder)
				if(matchBuilder.satisfiesAllMatches) {
					val grpStr = groupStrBuilder.buildGroupString
					indexUpdateBuilder.executeUpdates(grpStr)
					indexUpdateBuilder.reset()
				}
				cnt += 1
			}
		} catch {
			case e: Exception => e.printStackTrace
		} finally {
			tsBuffer.buffer.close()
			segmentBuffers.foreach(_.buffer.close())
		}
		timer.stop("Processed " + cnt + " Events")
	}

	def readNextFromBuffers(segmentBuffers: Iterable[SegmentBuffer], tsBuffer: SegmentBuffer, matchBuilder: MatchBuilder, grpStringBuilder: GroupStringBuilder, indexUpdateBuilder: IndexUpdateBuilder) {
		tsBuffer.buffer.readBytes(lngArr, 8)
		val timestamp = toLong(lngArr)

		segmentBuffers.foreach { segBuf =>
			readSegmentBasedOnType(segBuf, matchBuilder, grpStringBuilder, indexUpdateBuilder)
		}

		grpStringBuilder.applyTimestamp(timestamp)
	}

	def readSegmentBasedOnType(segmentBuffer: SegmentBuffer, matchBuilder: MatchBuilder, grpStringBuilder: GroupStringBuilder, indexUpdateBuilder: IndexUpdateBuilder) {
		val segment = segmentBuffer.segment
		val buffer = segmentBuffer.buffer
		val byte = buffer.readByte
		if(byte == 0) {
			matchBuilder.applySegment(segment)
			grpStringBuilder.applySegment(segment)
		} else if(byte == 1) {
			buffer.readBytes(lngArr,8)
			val dbl = toDouble(lngArr)
			matchBuilder.applySegment(segment, dbl)
			grpStringBuilder.applySegment(segment, dbl)
			indexUpdateBuilder.applySegment(segment, dbl)
		} else if(byte == 2) {
			val len = buffer.readByte
			val str = new String(buffer.getBytes(len))
			matchBuilder.applySegment(segment, str)
			grpStringBuilder.applySegment(segment, str)
			indexUpdateBuilder.applySegment(segment,str)
		} else {
			// Unrecognized Byte! - Probably bad!
		}
	}

	def createSegmentBuffers(segments: Iterable[String]): Iterable[SegmentBuffer] = {
		segments.filter(dataSegments.contains(_)).map(new SegmentBuffer(_, blade))
	}

	def retrieveRequiredSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		keys.head.matches.map(_.segment) ++: keys.head.groupings.map(_.segment)
	}

	def retrieveOptionalSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		keys.map(_.reducer.segment)
	}

	class SegmentBuffer(val segment: String, val blade: Blade) {
		val buffer = new CustomByteBuffer(getDataFileNameForBladeSegment(blade, segment), DEFAULT_PAGE_SIZE)
	}

	class EventOutputWriter(dataPartSegments: List[String], events: Iterable[Event]) {
		val newSegments = mutable.ListBuffer[String]()
		val outputStreamMap = mutable.Map[String,BufferedOutputStream]()
		initializeOutputStreamMap()

		def writeEvents() {
			val allEventSegments = events.flatMap(getEventSegments(_))
			addNewSegments(allEventSegments)
			events foreach { event =>
				writeEvent(event)
			}
		}

		private def writeEvent(event: Event) {
			outputStreamMap foreach { case (segment, outStream) =>
				if(segment == "ts") {
					outStream.write(bytes(event.ts))
				} else if(segment == "its") {
					outStream.write(bytes(event.its))
				} else {
					writeEventSegment(event, segment, outStream)
				}
			}
		}

		def close() {
			outputStreamMap.values.foreach { stream => stream.close() }
		}

		private def writeEventSegment(event: Event, segment: String, outStream: BufferedOutputStream) {
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

		private def initializeOutputStreamMap() {
			dataPartSegments.foreach { segment =>
				outputStreamMap(segment) = new BufferedOutputStream ( 
					new FileOutputStream(getDataFileNameForBladeSegment(blade, segment), true), 128 * 100
				)
			}
		}

		private def createOutputStream(segment: String): BufferedOutputStream = {
			new BufferedOutputStream ( 
				new FileOutputStream(getDataFileNameForBladeSegment(blade, segment), true), 128 * 100
			)
		}

		private def initializeSegment(segment: String) {
			ensureCacheSegmentFileExists(blade, segment)
			padSegmentFileZeroBytes(blade, segment, eventCount)
		}

		private def addNewSegments(segments: Iterable[String]) = {
			segments.filterNot(dataPartSegments.contains).foreach { segmentToAdd =>
				ensureCacheSegmentFileExists(blade, segmentToAdd)
				padSegmentFileZeroBytes(blade, segmentToAdd, eventCount)
				newSegments += segmentToAdd
			}
		}

		private def getEventSegments(event: Event): Iterable[String] = {
			(event.dblValues.keySet ++ event.strValues.keySet) + "ts" + "its"
		}
	}
}








