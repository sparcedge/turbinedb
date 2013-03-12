package com.sparcege.turbine.data

import org.scalatest.{WordSpec,BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers

import com.sparcedge.turbine.data._
import com.sparcedge.turbine.query._
import com.sparcedge.turbine.util.Timer
import com.sparcedge.turbine.{Blade,Collection}

class GroupStringBuilderPerfSpec extends WordSpec with MustMatchers {
	val timer = new Timer	
	val executionTimes = 100000
	QueryUtil.DATA_GROUPING = new IndexGrouping("hour")

	"A GroupStringBuilder" when {
		"processing duration groupings" when {
			"warmup cycle" should {
				GSBPerfHelper.processYearGrouping(executionTimes)
				GSBPerfHelper.processMonthGrouping(executionTimes)
				GSBPerfHelper.processMonthGrouping(executionTimes)
				GSBPerfHelper.processDayGrouping(executionTimes)
				GSBPerfHelper.processHourGrouping(executionTimes)
				GSBPerfHelper.processMinuteGrouping(executionTimes)
				"run" in {
				}
			}
			"year" should {
				timer.start()
				GSBPerfHelper.processYearGrouping(executionTimes)
				val runTime = timer.diff
				s"execute 100k times in ${runTime} ms" in {
					true must be (true)
				}
			}
			"month" should {
				timer.start()
				GSBPerfHelper.processMonthGrouping(executionTimes)
				val runTime = timer.diff
				s"execute 100k times in ${runTime} ms" in {
					true must be (true)
				}
			}
			"day" should {
				timer.start()
				GSBPerfHelper.processDayGrouping(executionTimes)
				val runTime = timer.diff
				s"execute 100k times in ${runTime} ms" in {
					true must be (true)
				}
			}
			"hour" should {
				timer.start()
				GSBPerfHelper.processHourGrouping(executionTimes)
				val runTime = timer.diff
				s"execute 100k times in ${runTime} ms" in {
					true must be (true)
				}
			}
			"minute" should {
				timer.start()
				GSBPerfHelper.processMinuteGrouping(executionTimes)
				val runTime = timer.diff
				s"execute 100k times in ${runTime} ms" in {
					true must be (true)
				}
			}
		}
	}
}

object GSBPerfHelper {
	val testBlade = Blade(Collection("test","test"), "2013-01")

	def processYearGrouping(times: Int) {
		val ts = System.currentTimeMillis
		val builder = new GroupStringBuilder( List(new YearDurationGrouping(None)), testBlade )
		var cnt = 0
		while(cnt < times) {
			builder("ts", ts)
			builder.buildGroupString
			cnt += 1
		}
	}

	def processMonthGrouping(times: Int) {
		val ts = System.currentTimeMillis
		val builder = new GroupStringBuilder( List(new MonthDurationGrouping(None)), testBlade )
		var cnt = 0
		while(cnt < times) {
			builder("ts", ts)
			builder.buildGroupString
			cnt += 1
		}
	}

	def processDayGrouping(times: Int) {
		val ts = System.currentTimeMillis
		val builder = new GroupStringBuilder( List(new DayDurationGrouping(None)), testBlade )
		var cnt = 0
		while(cnt < times) {
			builder("ts", ts)
			builder.buildGroupString
			cnt += 1
		}
	}

	def processHourGrouping(times: Int) {
		val ts = System.currentTimeMillis
		val builder = new GroupStringBuilder( List(new HourDurationGrouping(None)), testBlade )
		var cnt = 0
		while(cnt < times) {
			builder("ts", ts)
			builder.buildGroupString
			cnt += 1
		}
	}

	def processMinuteGrouping(times: Int) {
		val ts = System.currentTimeMillis
		val builder = new GroupStringBuilder( List(new MinuteDurationGrouping(None)), testBlade )
		var cnt = 0
		while(cnt < times) {
			builder("ts", ts)
			builder.buildGroupString
			cnt += 1
		}
	}
}