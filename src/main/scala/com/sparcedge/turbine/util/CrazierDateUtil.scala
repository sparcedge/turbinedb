package com.sparcedge.turbine.util

object CrazierDateUtil {

	val MILLIS_PER_SECOND = 1000L
	val MILLIS_PER_MINUTE = 6000L
	val MILLIS_PER_HOUR = 3600000L
	val MILLIS_PER_DAY = 86400000L
	val MILLIS_PER_YEAR = (365.2425 * MILLIS_PER_DAY).toLong
	val DAYS_0000_TO_1970 = 719527
	val MILLIS_AT_EPOCH_DIV_TWO = (1970L * MILLIS_PER_YEAR) / 2
	val YEAR_MILLIS_DIV_TWO = (MILLIS_PER_YEAR / 2).toLong
	val CACHE_SIZE = 1 << 10
    val CACHE_MASK = CACHE_SIZE - 1
    val YEAR_MILLIS_CACHE = new Array[Long](CACHE_SIZE)

	val MIN_DAYS_PER_MONTH_ARRAY = Array[Int] (
        31,28,31,30,31,30,31,31,30,31,30,31
    )
    val MAX_DAYS_PER_MONTH_ARRAY = Array[Int] (
        31,29,31,30,31,30,31,31,30,31,30,31
    )
	val MIN_TOTAL_MILLIS_BY_MONTH_ARRAY = new Array[Long](12)
	val MAX_TOTAL_MILLIS_BY_MONTH_ARRAY = new Array[Long](12)
	populateMinAndMaxArrays()
	
	private def populateMinAndMaxArrays() {
		var minSum = 0L
		var maxSum = 0L
		var cnt = 0

		while(cnt < 11) {
			var millis = MIN_DAYS_PER_MONTH_ARRAY(cnt) * MILLIS_PER_DAY
			minSum += millis
			MIN_TOTAL_MILLIS_BY_MONTH_ARRAY(cnt + 1) = minSum
   
			millis = MAX_DAYS_PER_MONTH_ARRAY(cnt) * MILLIS_PER_DAY
			maxSum += millis
			MAX_TOTAL_MILLIS_BY_MONTH_ARRAY(cnt + 1) = maxSum
			cnt += 1
		}
	}

	def calculateAbsoluteMinute(ms: Long): Long = {
		ms / MILLIS_PER_MINUTE
	}

	def calculateAbsoluteHour(ms: Long): Long = {
		ms / MILLIS_PER_HOUR
	}

	def calculateYearCombined(ms: Long): Long = {
		calculateYear(ms).toLong
	}

	def calculateMonthCombined(ms: Long): Long = {
		val year = calculateYear(ms)
		concat(year.toLong, calculateMonth(ms, year))
	}

	def calculateDayCombined(ms: Long): Long = {
		val year = calculateYear(ms)
		val month = calculateMonth(ms, year)
		concat(concat(year.toLong, month), calculateDay(ms, month, year))
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

	def calculateYear(ms: Long): Int = {
		val unitMillis = YEAR_MILLIS_DIV_TWO
		var i2 = (ms >> 1) + MILLIS_AT_EPOCH_DIV_TWO
		
		if(i2 < 0) {
			i2 = i2 - unitMillis + 1
		}
		var year = (i2 / unitMillis).toInt

		var yearStart = getYearMillisCached(year)
		val diff = ms - yearStart

		if(diff < 0) {
			year -= 1
		} else if(diff >= MILLIS_PER_DAY * 365L) {
			var oneYear = 0L
			if(isLeapYear(year)) {
				oneYear = MILLIS_PER_DAY * 366L
			} else {
				oneYear = MILLIS_PER_DAY * 365L
			}
			yearStart += oneYear

			if(yearStart <= ms) {
				year += 1
			}
		}

		year
	}

	def calculateMonth(ms: Long, year: Int): Int = {
		val i = ((ms - getYearMillis(year)) >> 10).toInt
		if(isLeapYear(year)) {
			if(i < 182 *84375) {
				if(i < 91 * 84375) {
					if(i < 31 * 84375) {
						1
					} else if(i < 60 * 84375) {
						2
					} else {
						3
					}
				} else {
					if(i < 121 * 84375) {
						4
					} else if(i < 152 * 84375) {
						5
					} else {
						6
					}
				}
			} else {
				if(i < 274 *84375) {
					if(i < 213 * 84375) {
						7
					} else if(i < 244 * 84375) {
						8
					} else {
						9
					}
				} else {
					if(i < 305 * 84375) {
						10
					} else if(i < 335 * 84375) {
						11
					} else {
						12
					}
				}
			}
		} else {
			if(i < 181 * 84375) {
				if(i < 90 * 84375) {
					if(i < 31 * 84375) {
						1
					} else if(i < 59 * 84375) {
						2
					} else {
						3
					}
				} else {
					if(i < 120 * 84375) {
						4
					} else if(i < 151 * 84375) {
						5
					} else {
						6
					}					
				}
			} else {
				if(i < 273 * 84375) {
					if(i < 212 * 84375) {
						7
					} else if(i < 243 * 84375) {
						8
					} else {
						9
					}
				} else {
					if(i < 304 * 84375) {
						10
					} else if(i < 334 * 84375) {
						11
					} else {
						12
					}
				}
			}
		}
	}

	def calculateDay(ms: Long, month: Int, year: Int): Int = {
		var dateMillis = getYearMillis(year)
		dateMillis += getTotalMillisByYearMonth(year, month)
		((ms - dateMillis) / MILLIS_PER_DAY).toInt + 1
	}

	def calculateHour(ms: Long): Int = {
		((ms / MILLIS_PER_HOUR) % 24).toInt
	}

	def calculateMinute(ms: Long): Int = {
		((ms / MILLIS_PER_MINUTE) % 60).toInt
	}

	def getTotalMillisByYearMonth(year: Int, month: Int): Long = {
		if (isLeapYear(year)) {
			MAX_TOTAL_MILLIS_BY_MONTH_ARRAY(month - 1)
		} else {
			MIN_TOTAL_MILLIS_BY_MONTH_ARRAY(month - 1)
		}
	}

    def getYearMillisCached(year: Int): Long = {
    	var millis = YEAR_MILLIS_CACHE(year & CACHE_MASK)
    	if(millis <= 0L) {
    		millis = getYearMillis(year)
    		YEAR_MILLIS_CACHE(year & CACHE_MASK) = millis
    	}
    	millis
    }

	def getYearMillis(year: Int): Long = {
		var leapYears = year / 100
		if(year < 0) {
			leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1
		} else {
			leapYears = (year >> 2) - leapYears + (leapYears >> 2)
			if (isLeapYear(year)) {
				leapYears -= 1
			}
		}

		(year * 365L + (leapYears - DAYS_0000_TO_1970)) * MILLIS_PER_DAY
	}

	def isLeapYear(year: Int): Boolean = {
		((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0)
	}

}