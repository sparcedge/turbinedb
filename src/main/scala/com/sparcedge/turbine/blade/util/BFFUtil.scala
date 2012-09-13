package com.sparcedge.turbine.blade.util

import java.io._
import java.nio.channels.FileChannel
import java.nio.ByteBuffer

import com.sparcedge.turbine.blade.event.{Event,LazyEvent}
import com.sparcedge.turbine.blade.query.Blade

object BFFUtil {

	var BASE_PATH = "cache"

	def serializeAndAddEvents(events: List[Event], blade: Blade) {
		val fileName = getFileNameForSegment(blade)
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream(fos, 128 * 100)
		val numEvents = events.size
		var cnt = 0
		while(cnt < numEvents) {
			val eventBytes = eventToBytes(event)
			bfos.write(BinaryUtil.intToBytes(eventBytes.size))
			bfos.write(eventBytes)
			cnt += 1
		}
	}

	def serializeAndAddEvents(cursor: MongoCursor, blade: Blade): Long = {
		val fileName = getFileNameForSegment(blade)
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream(fos, 128 * 100)
		var newestTimestamp = 0L
		cursor foreach { rawEvent =>
			val its: Long = event("its") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
				case _ => 0L
			}
			if(its > newestTimestamp) {
				newestTimestamp = its
			}

			val eventBytes = eventToBytes(ConcreteEvent.fromRawEvent(rawEvent))
			bfos.write(BinaryUtil.intToBytes(eventBytes.size))
			bfos.write(eventBytes)
			cnt += 1
		}
		newestTimestamp
	}

	def updateCacheMetadata(blade: Blade, timestamp: Long) {
		// TODO: Implement Meta Data
	}

	def processCachedEvents(blade: Blade)(processFun: (Event) => Unit) {
		val inChannel = new RandomAccessFile(fileName, "r").getChannel
		val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)

		try {
			while (buffer.hasRemaining) {
				processFun(readEventFromBuffer(buffer))
			}
		} catch {
			case e: Exception => e.printStackTrace
		} finally {
			buffer.clear()
			inChannel.close()
		}
	}

	private def readSerializedEventFromBuffer(buffer: ByteBuffer): LazyEvent = {
		val eSize = buffer.getInt
		val eventArr = new Array[Byte](eSize)
		buffer.get(eventArr)
		new LazyEvent(eventArr)
	}

	def getDirectoryForSegment(blade: Blade): String = {
		BASE_PATH + "/" + blade.domain + "/" + blade.tenant + "/" + blade.category
	}

	def getDataFileNameForSegment(blade: Blade): String = {
		getDirectoryForSegment(blade) + "/" + blade.period + ".data"
	}

	def getMetaFileNameForSegment(blade: Blade): String = {
		getDirectoryForSegment(blade) + "/" + blade.period + ".meta"
	}

	def ensureCacheFileExists(blade: Blade) {
		ensureCacheDirectoryExists(blade)
		val cacheFile = new File(getDataFileNameForSegment(blade))
		if(!cacheFile.exists) {
			cacheFile.createNewFile()
		}
	}

	def ensureMetaFileExists(blade: Blade) {
		ensureCacheDirectoryExists(blade)
		val metaFile = new File(getMetaFileNameForSegment(blade))
		if(!metaFile.exists) {
			metaFile.createNewFile()
		}
	}

	def ensureCacheDirectoryExists(blade: Blade) {
		new File(getDirectoryForSegment(blade)).mkdirs()
	}

	def retrieveBladesFromExistingData(): List[Blade] = {
		val cacheDir = new File(BASE_PATH)
		val cacheFilesAndDirs = recursiveListFilesAndDirs(cacheDir)
		val cacheFiles = cacheFilesAndDirs.filter(_.getName.contains(".data"))
		cacheFiles.map(convertCacheFileToBlade(_))
	}

	def convertCacheFileToBlade(cacheFile: File): Blade = {
		val path = cacheFile.getAbsolutePath
		val tokens = path.substring(path.indexOf(BASE_PATH) + BASE_PATH.size).split("/")
		new Blade(tokens(0), tokens(1), tokens(2), tokens(3).takeWhile(_ != '.'))
	}

	def recursiveListFilesAndDirs(f: File): Array[File] = {
		val subItems = Option(f.listFiles).getOrElse(Array[File]())
		subItems ++ subItems.filter(_.isDirectory).flatMap(recursiveListFilesAndDirs)
	}

}