package com.sparcedge.turbine.blade.util

import java.io._
import com.sparcedge.turbine.blade.event.{Event,LazyEvent,ConcreteEvent}
import com.sparcedge.turbine.blade.query.Blade

object DiskUtil {

	var BASE_PATH = "turbine-data"
	val DEFAULT_PAGE_SIZE = 1024 * 256

	def getDirectoryForBlade(blade: Blade): String = {
		BASE_PATH + "/" + blade.domain + "/" + blade.tenant + "/" + blade.category + "/" + blade.period + "-data"
	}

	def getDataFileNameForBladeSegment(blade: Blade, segment: String): String = {
		getDirectoryForBlade(blade) + "/" + segment + ".data"
	}

	def getMetaFileNameForBlade(blade: Blade): String = {
		getDirectoryForBlade(blade) + "/" + blade.period + ".meta"
	}

	def cacheSegmentFileExists(blade: Blade, segment: String): Boolean = {
		new File(getDataFileNameForBladeSegment(blade, segment)).exists
	}

	def metaDataFileExists(blade: Blade): Boolean = {
		new File(getMetaFileNameForBlade(blade)).exists
	}

	def padSegmentFileZeroBytes(blade: Blade, segment: String, count: Int) = {
		val segmentFile = new RandomAccessFile(getDataFileNameForBladeSegment(blade, segment), "rw")
		val zeroBytes = new Array[Byte](count)
		segmentFile.write(zeroBytes, 0, zeroBytes.size)
		segmentFile.close
	}

	def ensureCacheSegmentFileExists(blade: Blade, segment: String) {
		ensureCacheDirectoryExists(blade)
		val cacheFile = new File(getDataFileNameForBladeSegment(blade, segment: String))
		if(!cacheFile.exists) {
			cacheFile.createNewFile()
		}
	}

	def ensureMetaFileExists(blade: Blade) {
		ensureCacheDirectoryExists(blade)
		val metaFile = new File(getMetaFileNameForBlade(blade))
		if(!metaFile.exists) {
			metaFile.createNewFile()
			updateCacheMetadata(blade, new BladeMetaData(0L))
		}
	}

	def updateCacheMetadata(blade: Blade, bladeMeta: BladeMetaData) {
		val fileName = getMetaFileNameForBlade(blade)
		val file = new RandomAccessFile(fileName, "rw")
		val bytes = BinaryUtil.longToBytes(bladeMeta.timestamp)
		file.write(bytes, 0, bytes.size)
		file.close
	}

	def readBladeMetaFromDisk(blade: Blade): BladeMetaData = {
		val fileName = getMetaFileNameForBlade(blade)
		val file = new RandomAccessFile(fileName, "r")
		val bytes = new Array[Byte](8)
		file.read(bytes)
		val bladeMeta = new BladeMetaData(BinaryUtil.bytesToLong(bytes))
		file.close
		bladeMeta
	}

	def ensureCacheDirectoryExists(blade: Blade) {
		new File(getDirectoryForBlade(blade)).mkdirs()
	}

	def retrieveAllExistingSegments(blade: Blade): Iterable[String] = {
		val segments = new File(getDirectoryForBlade(blade)).listFiles.map(_.getName.takeWhile(_ != '.'))
		if(segments.contains("ts")) {
			segments
		} else {
			"ts" :: segments.toList
		}
	}

	def retrieveExistingEventCount(blade: Blade): Int = {
		val tsFile = new File(getDataFileNameForBladeSegment(blade, "ts"))
		if(tsFile.exists) {
			(tsFile.length / 8).toInt
		} else {
			0
		}
	}

	// TODO: Disambiguate -data
	def retrieveBladesFromExistingData(): Iterable[Blade] = {
		val cacheDir = new File(BASE_PATH)
		val cacheFilesAndDirs = recursiveListFilesAndDirs(cacheDir)
		val cacheFiles = cacheFilesAndDirs.filter(_.getName.contains("0-data"))
		cacheFiles.map(convertCacheFileToBlade(_))
	}

	def convertCacheFileToBlade(cacheFile: File): Blade = {
		val path = cacheFile.getAbsolutePath
		println(path)
		val tokens = path.substring(path.indexOf(BASE_PATH) + BASE_PATH.size).split("/")
		new Blade(tokens(1), tokens(2), tokens(3), tokens(4).substring(0,tokens(4).indexOf("-data")))
	}

	def recursiveListFilesAndDirs(f: File): Iterable[File] = {
		val subItems = Option(f.listFiles).getOrElse(Array[File]())
		subItems ++ subItems.filter(_.isDirectory).flatMap(recursiveListFilesAndDirs)
	}
}