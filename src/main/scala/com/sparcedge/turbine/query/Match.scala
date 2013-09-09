package com.sparcedge.turbine.query

import play.api.libs.json.{JsObject,JsNumber,JsString,JsValue,JsArray}
import scala.util.{Try,Success,Failure}

import com.sparcedge.turbine.data.{SegmentValueHolder,DataTypes}
import com.sparcedge.turbine.event.Event

object Match {
    def apply(jsObj: JsObject): Match = {
        jsObj.fields.head match {
            case (seg: String, matchObj: JsObject) => createMatch(seg, matchObj)
            case _ => throw new Exception("Invalid Match Structure")
        }
    }

    def createMatch(seg: String, matchObj: JsObject): Match = {
        matchObj.fields.head match {
            case ("eq", JsNumber(num)) => new EqualsNumericMatch(seg, num.toDouble)
            case ("eq", JsString(str)) => new EqualsStringMatch(seg, str)
            case ("ne", JsNumber(num)) => new NotEqualsNumericMatch(seg, num.toDouble)
            case ("ne", JsString(str)) => new NotEqualsStringMatch(seg, str)
            case ("gt", JsNumber(num)) => new GreaterThanNumericMatch(seg, num.toDouble)
            case ("gt", JsString(str)) => new GreaterThanStringMatch(seg, str)
            case ("gte", JsNumber(num)) => new GreaterThanEqualNumericMatch(seg, num.toDouble)
            case ("gte", JsString(str)) => new GreaterThanEqualStringMatch(seg, str)
            case ("lt", JsNumber(num)) => new LessThanNumericMatch(seg, num.toDouble)
            case ("lt", JsString(str)) => new LessThanStringMatch(seg, str)
            case ("lte", JsNumber(num)) => new LessThanEqualNumericMatch(seg, num.toDouble)
            case ("lte", JsString(str)) => new LessThanEqualStringMatch(seg, str)
            case ("re", JsString(str)) => new RegexMatch(seg, str)
            case ("in", JsArray(arr)) => new InMatch(seg, arr.map(unbox(_)))
            case ("nin", JsArray(arr)) => new InMatch(seg, arr.map(unbox(_)))
            case _ => throw new Exception("Invalid Match Type")
        }
    }

    def unbox(jsValue: JsValue): Any = {
        jsValue match {
            case JsNumber(num) => num
            case JsString(str) => str
            case _ => throw new Exception("Invalid Match Value")
        }
    }
}

// TODO: Add 'and' and 'or' matchers
abstract class Match(val segment: String) {
    var segmentPlaceholder = SegmentValueHolder(segment)
    val uniqueId: String

    def apply(placeholder: SegmentValueHolder) {
        if(placeholder.segment == segment) {
            segmentPlaceholder = placeholder
        }
    }

    def copy(): Match
    def evaluate(): Boolean
    def evaluate(e: Event): Boolean
}

abstract class NumericMatch(seg: String) extends Match(seg) {
    val value: Double

    def evaluate(): Boolean = segmentPlaceholder.isDouble && evaluate(segmentPlaceholder.getDouble)
    def evaluate(e: Event): Boolean = e.getDouble(segment).map(evaluate(_)).getOrElse(false)
    def evaluate(testVal: Double): Boolean
}

abstract class StringMatch(seg: String) extends Match(seg) {
    val value: String

    def evaluate(): Boolean = segmentPlaceholder.isString && evaluate(segmentPlaceholder.getString)
    def evaluate(e: Event): Boolean = e.getString(segment).map(evaluate(_)).getOrElse(false)
    def evaluate(testVal: String): Boolean
}

class EqualsNumericMatch(seg: String, val value: Double) extends NumericMatch(seg) {
    def evaluate(testVal: Double) = testVal == value
    def copy(): Match = new EqualsNumericMatch(segment, value)
    val uniqueId = s"EqualsNumericMatch.${segment}.${value}"
}

class EqualsStringMatch(seg: String, val value: String) extends StringMatch(seg) {
    def evaluate(testVal: String) = testVal == value
    def copy(): Match = new EqualsStringMatch(segment, value)
    val uniqueId = s"EqualsStringMatch.${segment}.${value}"
}

class NotEqualsNumericMatch(seg: String, val value: Double) extends NumericMatch(seg) {
    def evaluate(testVal: Double) = testVal != value
    def copy(): Match = new NotEqualsNumericMatch(segment, value)
    val uniqueId = s"NotEqualsNumericMatch.${segment}.${value}"
}

class NotEqualsStringMatch(seg: String, val value: String) extends StringMatch(seg) {
    def evaluate(testVal: String) = testVal != value
    def copy(): Match = new NotEqualsStringMatch(segment, value)
    val uniqueId = s"NotEqualsNumericMatch.${segment}.${value}"
}

class GreaterThanNumericMatch(seg: String, val value: Double) extends NumericMatch(seg) {
    def evaluate(testVal: Double) = testVal > value
    def copy(): Match = new GreaterThanNumericMatch(segment, value)
    val uniqueId = s"GreaterThanNumericMatch.${segment}.${value}"
}

class GreaterThanStringMatch(seg: String, val value: String) extends StringMatch(seg) {
    def evaluate(testVal: String) = testVal > value
    def copy(): Match = new GreaterThanStringMatch(segment, value)
    val uniqueId = s"GreaterThanStringMatch.${segment}.${value}"
}

class GreaterThanEqualNumericMatch(seg: String, val value: Double) extends NumericMatch(seg) {
    def evaluate(testVal: Double) = testVal >= value
    def copy(): Match = new GreaterThanEqualNumericMatch(segment, value)
    val uniqueId = s"GreaterThanEqualNumericMatch.${segment}.${value}"
}

class GreaterThanEqualStringMatch(seg: String, val value: String) extends StringMatch(seg) {
    def evaluate(testVal: String) = testVal >= value
    def copy(): Match = new GreaterThanEqualStringMatch(segment, value)
    val uniqueId = s"GreaterThanEqualStringMatch.${segment}.${value}"
}

class LessThanNumericMatch(seg: String, val value: Double) extends NumericMatch(seg) {
    def evaluate(testVal: Double) = testVal < value
    def copy(): Match = new LessThanNumericMatch(segment, value)
    val uniqueId = s"LessThanNumericMatch.${segment}.${value}"
}

class LessThanStringMatch(seg: String, val value: String) extends StringMatch(seg) {
    def evaluate(testVal: String) = testVal < value
    def copy(): Match = new LessThanStringMatch(segment, value)
    val uniqueId = s"LessThanStringMatch.${segment}.${value}"
}

class LessThanEqualNumericMatch(seg: String, val value: Double) extends NumericMatch(seg) {
    def evaluate(testVal: Double) = testVal <= value
    def copy(): Match = new LessThanEqualNumericMatch(segment, value)
    val uniqueId = s"LessThanEqualNumericMatch.${segment}.${value}"
}

class LessThanEqualStringMatch(seg: String, val value: String) extends StringMatch(seg) {
    def evaluate(testVal: String) = testVal <= value
    def copy(): Match = new LessThanEqualStringMatch(segment, value)
    val uniqueId = s"LessThanEqualStringMatch.${segment}.${value}"
}

class RegexMatch(seg: String, val value: String) extends StringMatch(seg) {
    if(!validRegex(value)) 
        throw new Exception("Invalid Regex Value")

    def evaluate(testVal: String): Boolean = matches(testVal)
    def matches(str: String): Boolean = str.matches(value)
    def validRegex(str: String): Boolean = Try(str.r).map(r => true).getOrElse(false)
    def copy(): Match = new RegexMatch(segment, value)
    val uniqueId = s"Regex.${segment}.${value}"
}

// TODO: Value Ordering
class InMatch(seg: String, values: Seq[Any]) extends Match(seg) {
    def evaluate(): Boolean = (segmentPlaceholder.getType) match {
        case DataTypes.NIL => false
        case DataTypes.NUMBER => values.contains(segmentPlaceholder.getDouble)
        case DataTypes.STRING => values.contains(segmentPlaceholder.getString)
        case DataTypes.TIMESTAMP => values.contains(segmentPlaceholder.getTimestamp)
        case _ => false
    }

    def evaluate(e: Event): Boolean = e(segment).map(values.contains(_)).getOrElse(false)
    def copy(): Match = new InMatch(segment, values)
    val uniqueId = s"InMatch.${segment}.${values.mkString}"
}

class NotInMatch(seg: String, values: Seq[Any]) extends Match(seg) {
    def evaluate(): Boolean = (segmentPlaceholder.getType) match {
        case DataTypes.NIL => true
        case DataTypes.NUMBER => !values.contains(segmentPlaceholder.getDouble)
        case DataTypes.STRING => !values.contains(segmentPlaceholder.getString)
        case DataTypes.TIMESTAMP => !values.contains(segmentPlaceholder.getTimestamp)
        case _ => false
    }

    def evaluate(e: Event): Boolean = e(segment).map(!values.contains(_)).getOrElse(true)
    def copy(): Match = new NotInMatch(segment, values)
    val uniqueId = s"InMatch.${segment}.${values.mkString}"
}