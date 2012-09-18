package com.sparcedge.turbine.blade.util

import java.io._
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor

import com.sparcedge.turbine.blade.event.{Event,LazyEvent,ConcreteEvent}
import com.sparcedge.turbine.blade.query.Blade

object BFFUtil {

	var BASE_PATH = "cache"

	def serializeAndAddEvents(events: List[Event], blade: Blade) {
		val fileName = getDataFileNameForSegment(blade)
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream(fos, 128 * 100)
		val numEvents = events.size
		var cnt = 0
		while(cnt < numEvents) {
			val eventBytes = BinaryUtil.eventToBytes(events(cnt))
			bfos.write(BinaryUtil.intToBytes(eventBytes.size))
			bfos.write(eventBytes)
			cnt += 1
		}
	}

	def serializeAndAddEvents(cursor: MongoCursor, blade: Blade): Long = {
		val timer = new Timer
		var cnt = 0
		val fileName = getDataFileNameForSegment(blade)
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream(fos, 128 * 100)
		var newestTimestamp = 0L
		timer.start()
		cursor foreach { rawEvent =>
			val its: Long = rawEvent("its") match { 
				case x: java.lang.Long => x
				case x: java.lang.Double => x.toLong 
				case _ => 0L
			}
			if(its > newestTimestamp) {
				newestTimestamp = its
			}

			val eventBytes = BinaryUtil.eventToBytes(ConcreteEvent.fromRawEvent(rawEvent))
			bfos.write(BinaryUtil.intToBytes(eventBytes.size))
			bfos.write(eventBytes)

			//cnt += 1
			//if(cnt % 1000 == 0) {
			//	println("Imported: " + cnt)
			//}
		}	
		timer.stop("[BFFUtil] Serialized " + cnt + " Events to File")
		newestTimestamp
	}

	def updateCacheMetadata(blade: Blade, timestamp: Long) {
		// TODO: Implement Meta Data
	}

	def processCachedEvents(blade: Blade)(processFun: (Event) => Unit) {
		val fileName = getDataFileNameForSegment(blade)
		val inChannel = new RandomAccessFile(fileName, "r").getChannel
		val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)
		val timer = new Timer
		var cnt = 0

		timer.start()
		try {
			while (buffer.hasRemaining) {
				processFun(readSerializedEventFromBuffer(buffer))
				cnt += 1
			}
		} catch {
			case e: Exception => //e.printStackTrace
		} finally {
			buffer.clear()
			inChannel.close()
		}
		timer.stop("[BFFUtil] Processed " + cnt + " Events")
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

	def doesCacheFileExist(blade: Blade): Boolean = {
		ensureCacheDirectoryExists(blade)
		val cacheFile = new File(getDataFileNameForSegment(blade))
		cacheFile.exists
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

	def retrieveBladesFromExistingData(): Iterable[Blade] = {
		val cacheDir = new File(BASE_PATH)
		val cacheFilesAndDirs = recursiveListFilesAndDirs(cacheDir)
		val cacheFiles = cacheFilesAndDirs.filter(_.getName.contains(".data"))
		cacheFiles.map(convertCacheFileToBlade(_))
	}

	def convertCacheFileToBlade(cacheFile: File): Blade = {
		val path = cacheFile.getAbsolutePath
		val tokens = path.substring(path.indexOf(BASE_PATH) + BASE_PATH.size).split("/")
		new Blade(tokens(1), tokens(2), tokens(3), tokens(4).takeWhile(_ != '.'))
	}

	def recursiveListFilesAndDirs(f: File): Iterable[File] = {
		val subItems = Option(f.listFiles).getOrElse(Array[File]())
		subItems ++ subItems.filter(_.isDirectory).flatMap(recursiveListFilesAndDirs)
	}

}