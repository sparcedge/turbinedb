package com.sparcedge.turbine.query

import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsString,JsValue,JsNumber}

import com.sparcedge.turbine.event.Event
import com.sparcedge.turbine.util.CrazierDateUtil._
import com.sparcedge.turbine.data.{SegmentValueHolder,DataTypes}

object Grouping {
    def apply(jsObj: JsObject): Grouping = {
        retrieveGroupingValue(jsObj) match {
            case Some(("segment", JsString(seg))) => new SegmentGrouping(seg)
            case Some(("duration", JsString(dur))) => DurationGrouping(dur, retrieveOffset(jsObj))
            case _ => throw new Exception("Invalid Grouping")
        }
    }

    def retrieveGroupingValue(jsObj: JsObject): Option[(String, JsValue)] = {
        jsObj.fields.find(f => f._1 == "segment" || f._1 == "duration")
    }

    def retrieveOffset(jsObj: JsObject): Option[Int] = {
        jsObj.fields.find(_._1 == "offset").map { field =>
            field match {
                case (_, JsNumber(num)) => num.toInt
                case _ => throw new Exception("Invalid offset value")
            }
        }
    }
}

abstract class Grouping(val segment: String) {
    var segmentPlaceholder = SegmentValueHolder(segment)

    def apply(placeholder: SegmentValueHolder) {
        if(placeholder.segment == segment) {
            segmentPlaceholder = placeholder
        }
    } 

    def evaluate(): String = (segmentPlaceholder.getType) match {
        case DataTypes.NIL => ""
        case DataTypes.NUMBER => segmentPlaceholder.getDouble.toString
        case DataTypes.STRING => segmentPlaceholder.getString
        case DataTypes.TIMESTAMP => segmentPlaceholder.getTimestamp.toString
        case _ => ""
    }

    def evaluate(e: Event): String = {
        e(segment).map(_.toString).getOrElse("")
    }

    def copy(): Grouping
    val uniqueId: String
}

class SegmentGrouping(segment: String) extends Grouping(segment) {
    def copy(): Grouping = new SegmentGrouping(segment)
    val uniqueId: String = s"SegmentGrouping.${segment}"
}

object DurationGrouping {
    def apply(duration: String, offsetOpt: Option[Int]): DurationGrouping = {
        val offset = offsetOpt.getOrElse(0)
        duration match {
            case "year" => new YearDurationGrouping(offset)
            case "month" => new MonthDurationGrouping(offset)
            case "day" => new DayDurationGrouping(offset)
            case "hour" => new HourDurationGrouping(offset)
            case "minute" => new MinuteDurationGrouping(offset)
            case _ => throw new Exception("Invalid Duration Value")
        }
    }
}

abstract class DurationGrouping(offset: Int) extends Grouping("ts") {
    val duration: String
    val uniqueId: String

    override def evaluate(): String = {
        val tsOffset = applyGmtOffset(segmentPlaceholder.getTimestamp, offset)
        calculateDurationValue(tsOffset).toString
    }

    override def evaluate(e: Event): String = {
        val tsOffset = applyGmtOffset(e.ts, offset)
        calculateDurationValue(tsOffset).toString
    }

    def calculateDurationValue(ts: Long): Long
}

class YearDurationGrouping(offset: Int) extends DurationGrouping(offset) {
    val duration = "year"

    def calculateDurationValue(ts: Long): Long = calculateYearCombined(ts)
    def copy(): Grouping = new YearDurationGrouping(offset)
    val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

class MonthDurationGrouping(offset: Int) extends DurationGrouping(offset) {
    val duration = "month"

    def calculateDurationValue(ts: Long): Long = calculateMonthCombined(ts)
    def copy(): Grouping = new MonthDurationGrouping(offset)
    val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

class DayDurationGrouping(offset: Int) extends DurationGrouping(offset) {
    val duration = "day"

    def calculateDurationValue(ts: Long): Long = calculateDayCombined(ts)
    def copy(): Grouping = new DayDurationGrouping(offset)
    val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

class HourDurationGrouping(offset: Int) extends DurationGrouping(offset) {
    val duration = "hour"

    def calculateDurationValue(ts: Long): Long = calculateHourCombined(ts)
    def copy(): Grouping = new HourDurationGrouping(offset)
    val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

class MinuteDurationGrouping(offset: Int) extends DurationGrouping(offset) {
    val duration = "minute"

    def calculateDurationValue(ts: Long): Long = calculateMinuteCombined(ts)
    def copy(): Grouping = new MinuteDurationGrouping(offset)
    val uniqueId: String = s"DurationGrouping.${duration}.${offset}"
}

object IndexGrouping {
    def apply(indexDuration: String, monthStart: Long): IndexGrouping = {
        if(indexDuration == "hour") {
            new HourIndexGrouping(monthStart)
        } else if(indexDuration == "minute") {
            new MinuteIndexGrouping(monthStart)
        } else {
            throw new Exception("Invalid Index Duration Value")
        }
    } 
}

abstract class IndexGrouping extends Grouping("ts") {
    override def evaluate(): String = {
        evaluate(segmentPlaceholder.getTimestamp)
    }
    override def evaluate(e: Event): String = {
        evaluate(e.ts)
    }
    def evaluate(millis: Long): String
}

class HourIndexGrouping(monthStart: Long) extends IndexGrouping {
    def evaluate(millis: Long): String = {
        calculateAbsoluteHourForMonth(millis, monthStart).toString
    }
    def copy(): Grouping = new HourIndexGrouping(monthStart)
    val uniqueId: String = s"IndexGrouping.hour"
}

class MinuteIndexGrouping(monthStart: Long) extends IndexGrouping {
    def evaluate(millis: Long): String = {
        calculateAbsoluteMinuteForMonth(millis, monthStart).toString
    }
    def copy(): Grouping = new MinuteIndexGrouping(monthStart)
    val uniqueId: String = s"IndexGrouping.minute"
}