package com.sparcedge.turbine.query

import play.api.libs.json.{JsObject,JsString,JsValue,JsArray}
import com.sparcedge.turbine.event.Event

object Extend {
	def apply(jsObj: JsObject): Extend = {
		jsObj.fields.head match {
			case (out: String, JsArray(extArr)) => createExtend(out, extArr)
			case _ => throw new Exception("Invalid Extend Structure")
		}
	}

	def createExtend(out: String, extArray: Seq[JsValue]): Extend = {
		val segmentIndexMap = extractSegments(extArray).zipWithIndex.toMap
		val rootElement = createExtendElement(extArray, segmentIndexMap)
		new Extend(out, rootElement, segmentIndexMap)
	}

	def createExtendElement(extArray: Seq[JsValue], segmentIndexMap: Map[String,Int]): ExtendElement = {
		(extArray.head, extArray.tail.map(createExtendElement(_, segmentIndexMap))) match {
			case (JsString("add"), elements) => AddExtendElement(elements.toArray)
			case (JsString("mul"), elements) => MultiplyExtendElement(elements.toArray)
			case (JsString("sub"), elements) if elements.size == 2 => SubtractExtendElement(elements(0), elements(1))
			case (JsString("div"), elements) if elements.size == 2 => DivideExtendElement(elements(0), elements(1))
			case _ => throw new Exception("Invalid Extend Query Element")
		}
	}

	def createExtendElement(extValue: JsValue, segmentIndexMap: Map[String,Int]): ExtendElement = {
		extValue match {
			case JsString(segment) => ValueExtendElement(segmentIndexMap(segment), segment)
			case JsArray(array) => createExtendElement(array, segmentIndexMap)
			case _ => throw new Exception("Invalid Extend Query Element")
		}
	}

	def extractSegments(extArray: Seq[JsValue]): Set[String] = {
		var segments = Set[String]()
		extArray.foreach { value =>
			value match {
				case JsString(seg) => segments += seg
				case JsArray(arr) => segments ++= extractSegments(arr)
				case _ => throw new Exception("Invalid Extend Query Element")
			}
		}
		segments -- List("add","sub","mul","div")
	}
}

class Extend(val out: String, val rootElement: ExtendElement, val segmentIndexMap: Map[String,Int]) {

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

trait ExtendElement {
	def evaluate(valArr: Array[Double]): Double
	def uniqueId: String
}

case class ValueExtendElement(idx: Int, segment: String) extends ExtendElement {
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

case class DivideExtendElement(elem1: ExtendElement, elem2: ExtendElement) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		val value2 = elem2.evaluate(valArr)
		if(value2 == 0) 0 else elem1.evaluate(valArr) / value2
	}
	val uniqueId = s"""div.${elem1.uniqueId}.${elem2.uniqueId}"""
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
	val uniqueId = s"""add.${elems.map(_.uniqueId).mkString(".")}"""
}

case class SubtractExtendElement(elem1: ExtendElement, elem2: ExtendElement) extends ExtendElement {
	def evaluate(valArr: Array[Double]): Double = {
		elem1.evaluate(valArr) - elem2.evaluate(valArr)
	}
	val uniqueId = s"""sub.${elem1.uniqueId}.${elem2.uniqueId}"""
}