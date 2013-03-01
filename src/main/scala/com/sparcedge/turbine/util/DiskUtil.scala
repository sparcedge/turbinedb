package com.sparcedge.turbine.util

import java.io._
import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.query.{Blade,Collection}
import BinaryUtil._

object DiskUtil {

	var BASE_PATH = "turbine-data"
	val DEFAULT_PAGE_SIZE = 1024 * 256

	def getDirectoryForBlade(blade: Blade): String = {
		BASE_PATH + "/" + blade.collection.domain + "/" + blade.collection.tenant + "/" + blade.collection.category + "/" + blade.period
	}

	def getDataFileNameForBladeSegment(blade: Blade, segment: String): String = {
		getDirectoryForBlade(blade) + "/" + segment + ".data"
	}

	def cacheSegmentFileExists(blade: Blade, segment: String): Boolean = {
		new File(getDataFileNameForBladeSegment(blade, segment)).exists
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

	def retrieveLatestInternalTimestamp(blade: Blade): Long = {
		val file = new File(getDataFileNameForBladeSegment(blade, "its"))
		val fileExistsAndNonEmpty = file.exists() && file.length > 0
		if(fileExistsAndNonEmpty) {
			val itsFile = new RandomAccessFile(getDataFileNameForBladeSegment(blade, "its"), "r")
			val bArr = new Array[Byte](8)
			val len = itsFile.length
			itsFile.seek(len-8)
			itsFile.read(bArr)
			itsFile.close()
			toLong(bArr)
		} else {
			0L
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

	def retrieveBladesFromExistingData(): Iterable[Blade] = {
		val cacheDir = new File(BASE_PATH)
		val cacheFilesAndDirs = recursiveListFilesAndDirs(cacheDir)
		val cacheFiles = cacheFilesAndDirs.filter(_.getName.matches("""\d{4}-\d{2}"""))
		cacheFiles.map(convertCacheFileToBlade(_))
	}

	def convertCacheFileToBlade(cacheFile: File): Blade = {
		val path = cacheFile.getAbsolutePath
		val tokens = path.substring(path.indexOf(BASE_PATH) + BASE_PATH.size).split("/")
		new Blade(Collection(tokens(1), tokens(2), tokens(3)), tokens(4))
	}

	def recursiveListFilesAndDirs(f: File): Iterable[File] = {
		val subItems = Option(f.listFiles).getOrElse(Array[File]())
		subItems ++ subItems.filter(_.isDirectory).flatMap(recursiveListFilesAndDirs)
	}
}