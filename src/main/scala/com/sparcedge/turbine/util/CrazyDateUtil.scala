package com.sparcedge.turbine.util

import org.joda.time.chrono.ISOChronology

object CrazyDateUtil {

	val chronology = ISOChronology.getInstanceUTC()

	val yearCalc = chronology.years()
	val monthCalc = chronology.monthOfYear()
	val dayCalc = chronology.dayOfMonth()
	val hourCalc = chronology.hourOfDay()
	val minCalc	= chronology.minuteOfHour()

	def calculateYear(ms: Long): Int = {
		1970 + yearCalc.getValue(ms)
	}

	def calculateMonth(ms: Long): Int = {
		monthCalc.get(ms)
	}

	def calculateDay(ms: Long): Int = {
		dayCalc.get(ms)
	}

	def calculateHour(ms: Long): Int = {
		hourCalc.get(ms)
	}

	def calculateMinute(ms: Long): Int = {
		minCalc.get(ms)
	}

	def calculateYearCombined(ms: Long): Long = {
		calculateYear(ms).toLong
	}

	def calculateMonthCombined(ms: Long): Long = {
		concat(calculateYearCombined(ms), calculateMonth(ms))
	}

	def calculateDayCombined(ms: Long): Long = {
		concat(calculateMonthCombined(ms), calculateDay(ms))
	}

	def calculateHourCombined(ms: Long): Long = {
		concat(calculateDayCombined(ms), calculateHour(ms))
	}

	def calculateMinuteCombined(ms: Long): Long = {
		concat(calculateHourCombined(ms), calculateMinute(ms))
	}

	def concat(base: Long, suffix: Int): Long = {
		(base * 100) + suffix
	}

	def concat(base: Long, suffix: Long): Long = {
		(base * 100) + suffix
	}	
}