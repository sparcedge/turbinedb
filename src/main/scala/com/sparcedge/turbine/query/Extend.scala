package com.sparcedge.turbine.query

import play.api.libs.json.{JsObject,JsString,JsValue,JsArray,JsNumber}
import com.sparcedge.turbine.event.Event

object Extend {
	def apply(jsObj: JsObject): Extend = {
		jsObj.fields.head match {
			case (out: String, JsArray(extArr)) => createExtend(out, extArr)
			case _ => throw new Exception("Invalid Extend Structure")
		}
	}

	def createExtend(out: String, extArray: Seq[JsValue]): Extend = {
		val segmentIndexMap = (List("@ts") ++ extractSegments(extArray)).zipWithIndex.toMap
		val rootElement = createExtendElement(extArray, segmentIndexMap)
		new Extend(out, rootElement, segmentIndexMap)
	}

	// TODO: Better way of identifying / creating elements
	def createExtendElement(extArray: Seq[JsValue], segmentIndexMap: Map[String,Int]): ExtendElement = {
		(extArray.head, extArray.tail.map(createExtendElement(_, segmentIndexMap))) match {
			case (JsString("add"), elements) => AddExtendElement(elements.toArray)
			case (JsString("mul"), elements) => MultiplyExtendElement(elements.toArray)
			case (JsString("sub"), elements) => SubtractExtendElement(elements.toArray)
			case (JsString("div"), elements) => DivideExtendElement(elements.toArray))
			case (JsString("yyyy"), elements) => YearDateExtendElement()
			case (JsString("yyyyMM"), elements) => YearMonthDateExtendElement()
			case (JsString("yyyyMMdd"), elements) => YearMonthDayDateExtendElement()
			case (JsString("yyyyMMddHH"), elements) => YearMonthDayHourDateExtendElement()
			case (JsString("yyyyMMddHHmm"), elements) => YearMonthDayHourMinuteDateExtendElement()
			case _ => throw new Exception("Invalid Extend Query Element")
		}
	}

	def createExtendElement(extValue: JsValue, segmentIndexMap: Map[String,Int]): ExtendElement = {
		extValue match {
			case JsNumber(num) => ValueExtendElement(num)
			case JsString(segment) => SegmentExtendElement(segmentIndexMap(segment), segment)
			case JsArray(array) => createExtendElement(array, segmentIndexMap)
			case _ => throw new Exception("Invalid Extend Query Element")
		}
	}

	def extractSegments(extArray: Seq[JsValue]): Set[String] = {
		var segments = Set[String]()
		extArray.foreach { value =>
			value match {
				case JsString(seg) if seg.startsWith("@") => segments += seg
				case JsArray(arr) => segments ++= extractSegments(arr)
				case _ => 
			}
		}
		segments
	}
}

class Extend(val out: String, val rootElement: ExtendElement, val segmentIndexMap: Map[String,Int]) extends QueryElement {

	val segments = segmentIndexMap.keys

	def evaluate(valArr: Array[Double]): Double = {
		rootElement.evaluate(valArr)
	}

	def uniqueId = rootElement.uniqueId

	def evaluate(event: Event): Option[(String,Double)] = {
		tryCreateValArrayFromEvent(event).map(out -> rootElement.evaluate(_))
	}

	private def tryCreateValArrayFromEvent(event: Event): Option[Array[Double]] = {
		val arr = new Array[Double](segments.size)
		var containsAll = true

		segmentIndexMap foreach { case (seg,idx) =>
			val value = event.getDoubleUnsafe(seg)
			if (value != null) arr(idx) = value else containsAll = false
		}

		if (containsAll) Some(arr) else None
	}
}

abstract class ExtendElement {
	def evaluate(valArr: Array[Double]): Double
	def uniqueId: String
}

case class ValueExtendElement(value: Double) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = value
	val uniqueId = value
}

case class SegmentExtendElement(idx: Int, segment: String) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = valArr(idx)
	val uniqueId = segment
}

case class MultiplyExtendElement(elems: Array[ExtendElement]) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		var product = elems(0).evaluate(valArr)
		var cnt = 1
		while(cnt < elems.size) {
			product *= elems(cnt).evaluate(valArr)
			cnt += 1
		}
		product
	}

	val uniqueId = s"""mul.${elems.map(_.uniqueId).mkString(".")}"""
}

case class DivideExtendElement(elems: Array[ExtendElement]) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		var div = elems(0).evaluate(valArr)
		var cnt = 1
		while(cnt < elems.size) {
			val value = elems(cnt).evaluate(valArr)
			if(value != 0) {
				div /= value 
			} else {
				div = 0
			}
			cnt += 1
		}
		div
	}
	val uniqueId = s"""add.${elems.map(_.uniqueId).mkString(".")}"""
}

case class AddExtendElement(elems: Array[ExtendElement]) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		var sum = elems(0).evaluate(valArr)
		var cnt = 1
		while(cnt < elems.size) {
			sum += elems(cnt).evaluate(valArr)
			cnt += 1
		}
		sum
	}
	val uniqueId = s"""div.${elems.map(_.uniqueId).mkString(".")}"""
}

case class SubtractExtendElement(elems: Array[ExtendElement]) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		var sub = elems(0).evaluate(valArr)
		var cnt = 1
		while(cnt < elems.size) {
			sub -= elems(cnt).evaluate(valArr)
			cnt += 1
		}
		sum
	}
	val uniqueId = s"""sub.${elems.map(_.uniqueId).mkString(".")}"""
}

abstract class DateExtendElement extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		evaluateTimestamp(valArr(0).toLong)
	}
}

case class YearDateExtendElement extends DateExtendElement {
	def evaluateTimestamp(ts: Long): Double = {

	}
	val uniqueId = s"""yyyy"""
}

case class YearMonthDateExtendElement extends DateExtendElement {
	def evaluateTimestamp(ts: Long): Double = {
		
	}
	val uniqueId = s"""yyyyMM.${elem1.uniqueId}.${elem2.uniqueId}"""
}

case class YearMonthDayDateExtendElement extends DateExtendElement {
	def evaluateTimestamp(ts: Long): Double = {
		
	}
	val uniqueId = s"""yyyyMMdd.${elem1.uniqueId}.${elem2.uniqueId}"""
}

case class YearMonthDayHourDateExtendElement extends DateExtendElement {
	def evaluateTimestamp(ts: Long): Double = {
		
	}
	val uniqueId = s"""yyyyMMddHH.${elem1.uniqueId}.${elem2.uniqueId}"""
}

case class YearMonthDayHourMinuteDateExtendElement extends DateExtendElement {
	def evaluateTimestamp(ts: Long): Double = {
		
	}
	val uniqueId = s"""yyyyMMddHHmm.${elem1.uniqueId}.${elem2.uniqueId}"""
}

// yyyy
// yyyyMM
// yyyyMMdd
// yyyyMMddHH
// yyyyMMddHHmm
// MinuteOfWeek
// MinuteOfDay
// MinuteOfMonth
// MinuteOfYear
// HourOfWeek
// HourOfDay
// HourOfMonth
// HourOfYear
// DayOfWeek
// DayOfMonth
// DayOfYear
// Month
// Year