package com.sparcege.turbine.query

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import com.sparcedge.turbine.util.CrazyDateUtil
import com.sparcedge.turbine.query._

class GroupingSpec extends WordSpec with MustMatchers {

	"A Segment Grouping" should {
		"return the same string value" in {
			val grouping = new SegmentGrouping("test")
			val value = "happy"
			grouping(value) must be (value)
		}
		"return the string version of a numeric value" in {
			val grouping = new SegmentGrouping("test")
			grouping(5.1) must be ("5.1")
		}
	}

	val ts = System.currentTimeMillis
	val millisInHour = 60*60*1000

	"A Duration Grouping" when {
		"year duration" should {
			"correctly calculate year" should {
				val grouping = new YearDurationGrouping(None)
				val jodaRes = CrazyDateUtil.calculateYearCombined(ts).toString
				grouping(ts) must be (jodaRes)
			}
			"correctly calculate year with offset applied" should {
				val grouping = new YearDurationGrouping(Some(-5))
				val offsetTs = ts - (5 * millisInHour)
				val jodaRes = CrazyDateUtil.calculateYearCombined(offsetTs).toString
				grouping(ts) must be (jodaRes)
			}
		}
		"month duration" when {
			"correctly calculate month" should {
				val grouping = new MonthDurationGrouping(None)
				val jodaRes = CrazyDateUtil.calculateMonthCombined(ts).toString
				grouping(ts) must be (jodaRes)
			}
			"correctly calculate month with offset applied" should {
				val grouping = new MonthDurationGrouping(Some(-5))
				val offsetTs = ts - (5 * millisInHour)
				val jodaRes = CrazyDateUtil.calculateMonthCombined(offsetTs).toString
				grouping(ts) must be (jodaRes)
			}
		}
		"day duration" when {
			"correctly calculate day" should {
				val grouping = new DayDurationGrouping(None)
				val jodaRes = CrazyDateUtil.calculateDayCombined(ts).toString
				grouping(ts) must be (jodaRes)
			}
			"correctly calculate day with offset applied" should {
				val grouping = new DayDurationGrouping(Some(-5))
				val offsetTs = ts - (5 * millisInHour)
				val jodaRes = CrazyDateUtil.calculateDayCombined(offsetTs).toString
				grouping(ts) must be (jodaRes)
			}
		}
		"hour duration" when {
			"correctly calculate hour" should {
				val grouping = new HourDurationGrouping(None)
				val jodaRes = CrazyDateUtil.calculateHourCombined(ts).toString
				grouping(ts) must be (jodaRes)
			}
			"correctly calculate hour with offset applied" should {
				val grouping = new HourDurationGrouping(Some(-5))
				val offsetTs = ts - (5 * millisInHour)
				val jodaRes = CrazyDateUtil.calculateHourCombined(offsetTs).toString
				grouping(ts) must be (jodaRes)
			}
		}
		"minute duration" when {
			"correctly calculate minute" should {
				val grouping = new MinuteDurationGrouping(None)
				val jodaRes = CrazyDateUtil.calculateMinuteCombined(ts).toString
				grouping(ts) must be (jodaRes)
			}
			"correctly calculate minute with offset applied" should {
				val grouping = new MinuteDurationGrouping(Some(-5))
				val offsetTs = ts - (5 * millisInHour)
				val jodaRes = CrazyDateUtil.calculateMinuteCombined(offsetTs).toString
				grouping(ts) must be (jodaRes)
			}
		}
	}
}