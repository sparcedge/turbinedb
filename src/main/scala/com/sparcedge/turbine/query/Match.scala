package com.sparcedge.turbine.query

import play.api.libs.json.{JsObject,JsNumber,JsString,JsValue,JsArray}
import scala.util.{Try,Success,Failure}

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

abstract class Match {
	val segment: String
	val uniqueId: String

	def apply(event: Event): Boolean = false
	def apply(str: String): Boolean = false
	def apply(numeric: Double): Boolean = false
	def apply(lng: Long): Boolean = false
	def apply(): Boolean = false
}

abstract class NumericMatch extends Match {
	val value: Double
	override def apply(event: Event): Boolean = event.getDouble(segment).map(evaluate(_)).getOrElse(false)
	override def apply(numeric: Double): Boolean = evaluate(numeric)
	override def apply(lng: Long): Boolean = evaluate(lng.toDouble)
	def evaluate(testVal: Double): Boolean
}

abstract class StringMatch extends Match {
	val value: String
	override def apply(event: Event): Boolean = event.getString(segment).map(evaluate(_)).getOrElse(false)
	override def apply(str: String): Boolean = evaluate(str)
	def evaluate(testVal: String): Boolean
}

class EqualsNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal == value
	val uniqueId = s"EqualsNumericMatch.${segment}.${value}"
}

class EqualsStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal == value
	val uniqueId = s"EqualsStringMatch.${segment}.${value}"
}

class NotEqualsNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal != value
	val uniqueId = s"NotEqualsNumericMatch.${segment}.${value}"
}

class NotEqualsStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal != value
	val uniqueId = s"NotEqualsNumericMatch.${segment}.${value}"
}

class GreaterThanNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal > value
	val uniqueId = s"GreaterThanNumericMatch.${segment}.${value}"
}

class GreaterThanStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal > value
	val uniqueId = s"GreaterThanStringMatch.${segment}.${value}"
}

class GreaterThanEqualNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal >= value
	val uniqueId = s"GreaterThanEqualNumericMatch.${segment}.${value}"
}

class GreaterThanEqualStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal >= value
	val uniqueId = s"GreaterThanEqualStringMatch.${segment}.${value}"
}

class LessThanNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal < value
	val uniqueId = s"LessThanNumericMatch.${segment}.${value}"
}

class LessThanStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal < value
	val uniqueId = s"LessThanStringMatch.${segment}.${value}"
}

class LessThanEqualNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal <= value
	val uniqueId = s"LessThanEqualNumericMatch.${segment}.${value}"
}

class LessThanEqualStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal <= value
	val uniqueId = s"LessThanEqualStringMatch.${segment}.${value}"
}

class RegexMatch(val segment: String, val regex: String) extends Match {
	if(!validRegex(regex)) 
		throw new Exception("Invalid Regex Value")

	override def apply(event: Event): Boolean = event.getString(segment).map(matches).getOrElse(false)
	override def apply(str: String): Boolean = matches(str)
	def matches(str: String): Boolean = str.matches(regex)
	def validRegex(str: String): Boolean = Try(str.r).map(r => true).getOrElse(false)
	val uniqueId = s"Regex.${segment}.${regex}"
}

// TODO: Value Ordering
class InMatch(val segment: String, val values: Seq[Any]) extends Match {
	override def apply(event: Event): Boolean = event(segment).map(values.contains(_)).getOrElse(false)
	override def apply(str: String): Boolean = values.contains(str)
	override def apply(numeric: Double): Boolean = values.contains(numeric)
	override def apply(lng: Long): Boolean = values.contains(lng)
	val uniqueId = s"InMatch.${segment}.${values.mkString}"
}

class NotInMatch(val segment: String, val values: Seq[Any]) extends Match {
	override def apply(event: Event): Boolean = event(segment).map(!values.contains(_)).getOrElse(false)
	override def apply(str: String): Boolean = !values.contains(str)
	override def apply(numeric: Double): Boolean = !values.contains(numeric)
	override def apply(lng: Long): Boolean = !values.contains(lng)
	val uniqueId = s"InMatch.${segment}.${values.mkString}"
}