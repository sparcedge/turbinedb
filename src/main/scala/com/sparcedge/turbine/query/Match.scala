package com.sparcedge.turbine.query

import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsNumber,JsString,JsValue,JsArray}

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

trait Match {
	val segment: String

	def apply(event: Event): Boolean = false
	def apply(str: String): Boolean = false
	def apply(numeric: Double): Boolean = false
	def apply(lng: Long): Boolean = false
	def apply(): Boolean = false
}

trait NumericMatch extends Match {
	val value: Double
	override def apply(event: Event): Boolean = event.getDouble(segment).map(evaluate(_)).getOrElse(false)
	override def apply(numeric: Double): Boolean = evaluate(numeric)
	override def apply(lng: Long): Boolean = evaluate(lng.toDouble)
	def evaluate(testVal: Double): Boolean
}

trait StringMatch extends Match {
	val value: String
	override def apply(event: Event): Boolean = event.getString(segment).map(evaluate(_)).getOrElse(false)
	override def apply(str: String): Boolean = evaluate(str)
	def evaluate(testVal: String): Boolean
}

class EqualsNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal == value
}

class EqualsStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal == value
}

class NotEqualsNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal != value
}

class NotEqualsStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal != value
}

class GreaterThanNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal > value
}

class GreaterThanStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal > value
}

class GreaterThanEqualNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal >= value
}

class GreaterThanEqualStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal >= value
}

class LessThanNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal < value
}

class LessThanStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal < value
}

class LessThanEqualNumericMatch(val segment: String, val value: Double) extends NumericMatch {
	def evaluate(testVal: Double) = testVal <= value
}

class LessThanEqualStringMatch(val segment: String, val value: String) extends StringMatch {
	def evaluate(testVal: String) = testVal <= value
}

class InMatch(val segment: String, val values: Seq[Any]) extends Match {
	override def apply(event: Event): Boolean = event(segment).map(values.contains(_)).getOrElse(false)
	override def apply(str: String): Boolean = values.contains(str)
	override def apply(numeric: Double): Boolean = values.contains(numeric)
	override def apply(lng: Long): Boolean = values.contains(lng)
}

class NotInMatch(val segment: String, val values: Seq[Any]) extends Match {
	override def apply(event: Event): Boolean = event(segment).map(!values.contains(_)).getOrElse(false)
	override def apply(str: String): Boolean = !values.contains(str)
	override def apply(numeric: Double): Boolean = !values.contains(numeric)
	override def apply(lng: Long): Boolean = !values.contains(lng)
}