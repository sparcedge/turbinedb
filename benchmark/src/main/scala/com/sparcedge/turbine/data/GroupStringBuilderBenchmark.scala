package com.sparcedge.turbine.data

import com.sparcedge.turbine.TurbineBenchmark
import com.sparcedge.turbine.data._
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.util.Timer
import com.sparcedge.turbine.{Blade,Collection}

class GroupStringBuilderBenchmark extends TurbineBenchmark {
	
	QueryUtil.DATA_GROUPING = new IndexGrouping("hour")
	val testBlade = Blade(Collection("test","test"), "2013-01")

	val yearBuilder = GroupStringBuilder ( List(new YearDurationGrouping(None)), testBlade )
	val monthBuilder = GroupStringBuilder ( List(new MonthDurationGrouping(None)), testBlade )
	val dayBuilder = GroupStringBuilder ( List(new DayDurationGrouping(None)), testBlade )
	val hourBuilder = GroupStringBuilder ( List(new HourDurationGrouping(None)), testBlade )
	val minuteBuilder = GroupStringBuilder ( List(new MinuteDurationGrouping(None)), testBlade )

	def timeYearGrpBuilder(reps: Int) = repeat(reps) {
		val ts = System.currentTimeMillis
		yearBuilder("ts", ts)
		yearBuilder.buildGroupString
	}

	def timeMonthGrpBuilder(reps: Int) = repeat(reps) {
		val ts = System.currentTimeMillis
		monthBuilder("ts", ts)
		monthBuilder.buildGroupString
	}

	def timeDayGrpBuilder(reps: Int) = repeat(reps) {
		val ts = System.currentTimeMillis
		dayBuilder("ts", ts)
		dayBuilder.buildGroupString	
	}

	def timeHourGrpBuilder(reps: Int) = repeat(reps) {
		val ts = System.currentTimeMillis
		hourBuilder("ts", ts)
		hourBuilder.buildGroupString	
	}

	def timeMinuteGrpBuilder(reps: Int) = repeat(reps) {
		val ts = System.currentTimeMillis
		minuteBuilder("ts", ts)
		minuteBuilder.buildGroupString	
	}
}