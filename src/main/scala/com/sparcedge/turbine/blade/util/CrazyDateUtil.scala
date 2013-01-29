package com.sparcedge.turbine.blade.util

import org.joda.time.chrono.ISOChronology
import org.joda.time.format.DateTimeFormat

object CrazyDateUtil {

	val chronology = ISOChronology.getInstanceUTC()
	val formatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH")

	def calculateYear(ms: Long): Int = {
		1970 + chronology.years().getValue(ms)
	}

	def calculateMonth(ms: Long): Int = {
		chronology.monthOfYear().get(ms)
	}

	def calculateDay(ms: Long): Int = {
		chronology.dayOfMonth().get(ms)
	}

	def calculateHour(ms: Long): Int = {
		chronology.hourOfDay().get(ms)
	}

	def calculateMinute(ms: Long): Int = {
		chronology.minuteOfDay().get(ms)
	}

	def calculateYearString(ms: Long): Long = {
		calculateYear(ms)
	}

	def calculateMonthString(ms: Long): Long = {
		concat(calculateYear(ms), calculateMonth(ms))
	}

	def calculateDayString(ms: Long): Long = {
		concat(concat(calculateYear(ms), calculateMonth(ms)), calculateDay(ms))
	}

	def calculateHourString(ms: Long): Long = {
		concat(concat(concat(calculateYear(ms), calculateMonth(ms)), calculateDay(ms)), calculateHour(ms))
	}

	def calculateMinuteString(ms: Long): Long = {
		concat(concat(concat(concat(calculateYear(ms), calculateMonth(ms)), calculateDay(ms)), calculateHour(ms)), calculateMinute(ms))
	}

	def concat(base: Int, suffix: Int): Long = {
		((base * 100) + suffix).toLong
	}

	def concat(base: Int, suffix: Long): Long = {
		(base * 100) + suffix
	}

	def concat(base: Long, suffix: Long): Long = {
		(base * 100) + suffix
	}	
}