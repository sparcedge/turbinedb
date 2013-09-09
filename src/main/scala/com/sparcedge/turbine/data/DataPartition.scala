package com.sparcedge.turbine.data

import java.io._
import scala.collection.mutable
import akka.actor.ActorRef

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.{DiskUtil,Timer,CustomByteBuffer}
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.query.pipeline._
import com.sparcedge.turbine.Blade

import com.sparcedge.turbine.util.DiskUtil._
import com.sparcedge.turbine.util.BinaryUtil._
import com.sparcedge.turbine.data.QueryUtil._
import AggregateIndex._

class DataPartition(val blade: Blade) {

	ensureCacheDirectoryExists(blade)

	val partitionDirectory = getDirectoryForBlade(blade)
	var dataSegments = retrieveAllExistingSegments(blade).toVector
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
	// TODO: Short circuit based on lack of required segments
	def populateIndexes(indexes: Iterable[Index]) {
		val keys = indexes.map(_.indexKey)
		val reqSegments = retrieveRequiredSegments(keys)
		val optSegments = retrieveOptionalSegments(keys)
		populateIndexes(indexes, (reqSegments ++ optSegments).toVector.distinct)
	}

	def populateIndexes(indexes: Iterable[Index], segments: Iterable[String]) {	
		val actionableSegments = segments.filter(seg => dataSegments.contains(seg) && seg != "ts")	
		val iKey = indexes.head.indexKey
		val extenders = iKey.extenders.map(ExtendPipelineElement(_))
		val matches = iKey.matches.map(MatchPipelineElement(_))
		val reduce = ReducePipelineElement(iKey.groupings, indexes, blade)
		val pipelineElements = (List[QueryPipelineElement]() ++ extenders ++ matches ++ List(reduce)).toArray
		
		val placeholders = actionableSegments.map(SegmentValueHolder(_)).toArray
		val segmentBuffers = actionableSegments.map(new SegmentBuffer(_, blade)).toArray
		val tsPlaceholder = SegmentValueHolder("ts")
		val tsBuffer = new SegmentBuffer("ts", blade)

		initializePipeline(pipelineElements, placeholders ++ List(tsPlaceholder))

		val timer = new Timer()
		timer.start()
		var cnt = 0
		try {	
			while(tsBuffer.buffer.hasRemaining) {
				readNextFromBuffers(segmentBuffers, tsBuffer, placeholders, tsPlaceholder)
				var idx = 0
				var shouldContinue = true
				while(idx < pipelineElements.length && shouldContinue) {
					val element = pipelineElements(idx)
					element.evaluate()
					shouldContinue = element.shouldContinue
					idx += 1
				}
				clearPlaceholders(placeholders)
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

	def readNextFromBuffers(segmentBuffers: Array[SegmentBuffer], tsBuffer: SegmentBuffer, placeholders: Array[SegmentValueHolder], tsPlaceholder: SegmentValueHolder) {
		tsBuffer.buffer.readBytes(lngArr, 8)
		val timestamp = toLong(lngArr)

		var idx = 0
		while(idx < segmentBuffers.length) {
			readSegmentBasedOnType(segmentBuffers(idx), placeholders(idx))
			idx += 1
		}

		tsPlaceholder(timestamp)
	}

	def readSegmentBasedOnType(segmentBuffer: SegmentBuffer, placeholder: SegmentValueHolder) {
		val segment = segmentBuffer.segment
		val buffer = segmentBuffer.buffer
		val byte = buffer.readByte
		if(byte == 0) {
			/* Nothing to do (placeholders already nil) */
		} else if(byte == 1) {
			buffer.readBytes(lngArr,8)
			val dbl = toDouble(lngArr)
			placeholder(dbl)
		} else if(byte == 2) {
			val len = buffer.readByte
			val str = new String(buffer.getBytes(len))
			placeholder(str)
		} else {
			// Unrecognized Byte! - Probably bad!
		}
	}

	def clearPlaceholders(placeholders: Array[SegmentValueHolder]) {
		var idx = 0
		while(idx < placeholders.length) {
			placeholders(idx).clear()
			idx += 1
		}
	}

	def initializePipeline(pipelineElements: Iterable[QueryPipelineElement], placeholders: Iterable[SegmentValueHolder]) {
		pipelineElements.foldLeft(placeholders) { (placeholders, element)  =>
			element(placeholders)
			placeholders ++ element.extendPlaceholders
		}
	}

	// TODO: Generic Based On Pipeline
	def retrieveRequiredSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		val key = keys.head
		key.extenders.map(_.segments).foldLeft(Vector[String]())(_++_) ++ key.matches.map(_.segment) ++ key.groupings.map(_.segment)
	}

	def retrieveOptionalSegments(keys: Iterable[IndexKey]): Iterable[String] = {
		keys.map(_.reducer.segment)
	}

	class SegmentBuffer(val segment: String, val blade: Blade) {
		val buffer = new CustomByteBuffer(getDataFileNameForBladeSegment(blade, segment), DEFAULT_PAGE_SIZE)
	}

	class EventOutputWriter(dataPartSegments: Vector[String], events: Iterable[Event]) {
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