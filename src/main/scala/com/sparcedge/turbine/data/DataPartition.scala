package com.sparcedge.turbine.data

import java.io._
import scala.collection.mutable
import akka.actor.ActorRef

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.{DiskUtil,Timer,CustomByteBuffer}
import com.sparcedge.turbine.query.{Match,Grouping}
import com.sparcedge.turbine.Blade

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

	// TODO: Prevent Query Processing of segments shadowed by extend values
	def populateIndexes(indexes: Iterable[Index]) {
		val keys = indexes.map(_.indexKey)
		val reqSegments = retrieveRequiredSegments(keys)
		val optSegments = retrieveOptionalSegments(keys)
		if(reqSegments.forall(dataSegments.contains(_)) && optSegments.exists(dataSegments.contains(_))) {
			populateIndexes(indexes, (reqSegments ++ optSegments).toList.distinct)
		}
	}

	def populateIndexes(indexes: Iterable[Index], segments: Iterable[String]) {
		val extenders = indexes.head.indexKey.extenders
		val matches = indexes.head.indexKey.matches
		val groupings = indexes.head.indexKey.groupings

		val extendBuilder = ExtendBuilder(extenders)
		val matchBuilder = MatchBuilder(matches)
		val groupStrBuilder = GroupStringBuilder(groupings, blade)
		val indexUpdateBuilder = IndexUpdateBuilder(indexes)

		val segmentBuffers = createSegmentBuffers(segments.toList) 
		val tsBuffer = new SegmentBuffer("ts", blade)
		val timer = new Timer()
		timer.start()
		var cnt = 0
		try {	
			while(tsBuffer.buffer.hasRemaining) {
				readNextFromBuffers(segmentBuffers, tsBuffer, extendBuilder, matchBuilder, groupStrBuilder, indexUpdateBuilder)
				applyExtendSegments(extendBuilder, matchBuilder, groupStrBuilder, indexUpdateBuilder)
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

	def readNextFromBuffers(segmentBuffers: Iterable[SegmentBuffer], tsBuffer: SegmentBuffer, extendBuilder: ExtendBuilder, 
									matchBuilder: MatchBuilder, grpStringBuilder: GroupStringBuilder, indexUpdateBuilder: IndexUpdateBuilder) {
		tsBuffer.buffer.readBytes(lngArr, 8)
		val timestamp = toLong(lngArr)

		segmentBuffers.foreach { segBuf =>
			readSegmentBasedOnType(segBuf, extendBuilder, matchBuilder, grpStringBuilder, indexUpdateBuilder)
		}

		grpStringBuilder("ts", timestamp)
	}

	def readSegmentBasedOnType(segmentBuffer: SegmentBuffer, extendBuilder: ExtendBuilder, 
			matchBuilder: MatchBuilder, grpStringBuilder: GroupStringBuilder, indexUpdateBuilder: IndexUpdateBuilder) {
		val segment = segmentBuffer.segment
		val buffer = segmentBuffer.buffer
		val byte = buffer.readByte
		if(byte == 0) {
			matchBuilder(segment)
			grpStringBuilder(segment)
		} else if(byte == 1) {
			buffer.readBytes(lngArr,8)
			val dbl = toDouble(lngArr)
			extendBuilder(segment, dbl)
			matchBuilder(segment, dbl)
			grpStringBuilder(segment, dbl)
			indexUpdateBuilder(segment, dbl)
		} else if(byte == 2) {
			val len = buffer.readByte
			val str = new String(buffer.getBytes(len))
			matchBuilder(segment, str)
			grpStringBuilder(segment, str)
			indexUpdateBuilder(segment,str)
		} else {
			// Unrecognized Byte! - Probably bad!
		}
	}

	def applyExtendSegments(extendBuilder: ExtendBuilder, matchBuilder: MatchBuilder, groupStrBuilder: GroupStringBuilder, indexUpdateBuilder: IndexUpdateBuilder) {
		val values = extendBuilder.extensionValues
		var cnt = 0
		while(cnt < values.length) {
			val valSeg = values(cnt)
			if(valSeg != null) {
				val seg = valSeg._1
				val value = valSeg._2
				matchBuilder(seg, value)
				groupStrBuilder(seg, value)
				indexUpdateBuilder(seg, value)
			}
			cnt += 1
		}
		extendBuilder.reset()
	}

	def createSegmentBuffers(segments: Iterable[String]): Iterable[SegmentBuffer] = {
		segments.filter(seg => dataSegments.contains(seg) && seg != "ts").map(new SegmentBuffer(_, blade))
	}

	def retrieveRequiredSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		val extendSegments = if(keys.head.extenders.map(_.segments).size > 0) keys.head.extenders.map(_.segments).reduce(_++_) else List[String]()
		keys.head.matches.map(_.segment) ++: keys.head.groupings.map(_.segment) ++: extendSegments
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
		val allEventSegments = events.flatMap(getEventSegments(_)).toSet
		
		addNewSegments(allEventSegments)
		initializeOutputStreamMap()

		def writeEvents() {
			
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
			(dataPartSegments ++ newSegments).foreach { segment =>
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