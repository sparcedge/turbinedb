package com.sparcedge.turbine.query

import play.api.libs.json.{JsObject,JsString,JsValue,JsArray}
import com.sparcedge.turbine.data.SegmentValueHolder
import com.sparcedge.turbine.event.Event

object Extend {
    def apply(jsObj: JsObject): Extend = {
        jsObj.fields.head match {
            case (out: String, JsArray(extArr)) => createExtend(out, extArr)
            case _ => throw new Exception("Invalid Extend Structure")
        }
    }

    def createExtend(out: String, extArray: Seq[JsValue]): Extend = {
        val rootElement = createExtendElement(extArray)
        new Extend(out, rootElement)
    }

    def createExtendElement(extArray: Seq[JsValue]): ExtendElement = {
        (extArray.head, extArray.tail.map(createExtendElement(_))) match {
            case (JsString("add"), elements) => AddExtendElement(elements.toArray)
            case (JsString("mul"), elements) => MultiplyExtendElement(elements.toArray)
            case (JsString("sub"), elements) if elements.size == 2 => SubtractExtendElement(elements(0), elements(1))
            case (JsString("div"), elements) if elements.size == 2 => DivideExtendElement(elements(0), elements(1))
            case _ => throw new Exception("Invalid Extend Query Element")
        }
    }

    def createExtendElement(extValue: JsValue): ExtendElement = {
        extValue match {
            case JsString(segment) => ValueExtendElement(segment)
            case JsArray(array) => createExtendElement(array)
            case _ => throw new Exception("Invalid Extend Query Element")
        }
    }
}

class Extend(val out: String, val rootElement: ExtendElement) {

    val extendPlaceholder = SegmentValueHolder(out)

    def evaluate() = {
        extendPlaceholder(rootElement.evaluate())
    }

    def evaluate(event: Event): Double = {
        rootElement.evaluate(event)
    }

    def apply(segmentPlaceholder: SegmentValueHolder) {
        rootElement(segmentPlaceholder)
    }

    def satisfied(): Boolean = {
        rootElement.satisfied()
    }

    def satisfied(e: Event): Boolean = {
        rootElement.satisfied(e)
    }

    def copy(): Extend = {
        new Extend(out, rootElement.copy())
    }

    def segments(): Iterable[String] = {
        rootElement.segments
    }

    def uniqueId = rootElement.uniqueId
}

// TODO: Define in/out data types (handle more than just numeric)
trait ExtendElement {
    def apply(segmentPlaceHolder: SegmentValueHolder)
    def evaluate(): Double
    def evaluate(e: Event): Double
    def satisfied(): Boolean
    def satisfied(e: Event): Boolean
    def copy(): ExtendElement
    def segments(): Iterable[String]
    def uniqueId: String
}

case class ValueExtendElement(segment: String) extends ExtendElement {
    var segmentPlaceholder = SegmentValueHolder(segment)
    def evaluate(): Double = segmentPlaceholder.getDouble
    def evaluate(e: Event): Double = e.getDoubleUnsafe(segment)
    def satisfied(): Boolean = segmentPlaceholder.isDouble()
    def satisfied(e: Event): Boolean = e.containsDouble(segment)
    val segments = Vector(segment)
    val uniqueId = segment

    def apply(placeholder: SegmentValueHolder) { 
        if(placeholder.segment == segment) {
            segmentPlaceholder = placeholder 
        }
    }

    def copy(): ExtendElement = ValueExtendElement(segment)
}

abstract class VarArgFunctionExtendElement(elems: Array[ExtendElement]) extends ExtendElement {
    def satisfied(): Boolean = {
        var i = 0
        var satisfied = true
        while(i < elems.length && satisfied) {
            satisfied = elems(i).satisfied()
            i += 1
        }
        satisfied
    }

    def satisfied(e: Event): Boolean = {
        var i = 0
        var satisfied = true
        while(i < elems.length && satisfied) {
            satisfied = elems(i).satisfied(e)
            i += 1
        }
        satisfied
    }

    def segments(): Iterable[String] = elems.foldLeft(Vector[String]())(_ ++ _.segments)

    def apply(segmentPlaceholder: SegmentValueHolder) {
        var i = 0
        while(i < elems.length && satisfied) {
            elems(i)(segmentPlaceholder)
            i += 1
        }
    }
}

abstract class TwoArgFunctionExtendElement(elem1: ExtendElement, elem2: ExtendElement) extends ExtendElement {
    def satisfied(): Boolean = {
        elem1.satisfied && elem2.satisfied
    }

    def satisfied(e: Event): Boolean = {
        elem1.satisfied(e) && elem2.satisfied(e)
    }

    def segments(): Iterable[String] = elem1.segments ++ elem2.segments

    def apply(segmentPlaceholder: SegmentValueHolder) {
        elem1(segmentPlaceholder)
        elem2(segmentPlaceholder)
    }
}

case class MultiplyExtendElement(elems: Array[ExtendElement]) extends VarArgFunctionExtendElement(elems) {
    def evaluate(): Double = {
        var product = elems(0).evaluate()
        var cnt = 1
        while(cnt < elems.size) {
            product *= elems(cnt).evaluate()
            cnt += 1
        }
        product
    }

    def evaluate(e: Event): Double = {
        var product = elems(0).evaluate(e)
        var cnt = 1
        while(cnt < elems.size) {
            product *= elems(cnt).evaluate(e)
            cnt += 1
        }
        product
    }

    def copy(): ExtendElement = MultiplyExtendElement(elems.map(_.copy()))

    val uniqueId = s"""mul.${elems.map(_.uniqueId).mkString(".")}"""
}

case class DivideExtendElement(elem1: ExtendElement, elem2: ExtendElement) extends TwoArgFunctionExtendElement(elem1, elem2) {
    def evaluate(): Double = {
        val value2 = elem2.evaluate()
        if(value2 == 0) 0 else elem1.evaluate() / value2
    }

    def evaluate(e: Event): Double = {
        val value2 = elem2.evaluate(e)
        if(value2 == 0) 0 else elem1.evaluate(e) / value2
    }

    def copy(): ExtendElement = DivideExtendElement(elem1.copy(), elem2.copy())

    val uniqueId = s"""div.${elem1.uniqueId}.${elem2.uniqueId}"""
}

case class AddExtendElement(elems: Array[ExtendElement]) extends VarArgFunctionExtendElement(elems) {
    def evaluate(): Double = {
        var sum = elems(0).evaluate()
        var cnt = 1
        while(cnt < elems.size) {
            sum += elems(cnt).evaluate()
            cnt += 1
        }
        sum
    }

    def evaluate(e: Event): Double = {
        var sum = elems(0).evaluate(e)
        var cnt = 1
        while(cnt < elems.size) {
            sum += elems(cnt).evaluate(e)
            cnt += 1
        }
        sum
    }

    def copy(): ExtendElement = AddExtendElement(elems.map(_.copy()))

    val uniqueId = s"""add.${elems.map(_.uniqueId).mkString(".")}"""
}

case class SubtractExtendElement(elem1: ExtendElement, elem2: ExtendElement) extends TwoArgFunctionExtendElement(elem1, elem2) {
    def evaluate(): Double = {
        elem1.evaluate() - elem2.evaluate()
    }

    def evaluate(e: Event): Double = {
        elem1.evaluate(e) - elem2.evaluate(e)
    }

    def copy(): ExtendElement = SubtractExtendElement(elem1.copy(), elem2.copy())

    val uniqueId = s"""sub.${elem1.uniqueId}.${elem2.uniqueId}"""
}