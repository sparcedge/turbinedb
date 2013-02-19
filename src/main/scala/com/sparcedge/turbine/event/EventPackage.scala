package com.sparcedge.turbine.event

import com.sparcedge.turbine.query.Blade

import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

object EventPackage {
	implicit val formats = org.json4s.DefaultFormats
	val formatter = DateTimeFormat.forPattern("yyyy-MM")

	def unmarshall(eventJson: String): EventPackage = {
		val json = parse(eventJson)
		val eiPackage = json.extract[EventIngressPackage]
		val event = convertIngressEventToEvent(eiPackage.event)
		val period = formatter.print(new DateTime(event.ts))
		EventPackage(Blade(eiPackage.domain, eiPackage.tenant, eiPackage.category, period), event)
	}

	def convertIngressEventToEvent(iEvent: IngressEvent): Event = {
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()

		iEvent.data.foreach { 
			case (segment, JString(value)) => strValues(segment) = value
			case (segment, JDouble(value)) => dblValues(segment) = value
			case (segment, JInt(value)) => dblValues(segment) = value.toDouble
			case (segment, JDecimal(value)) => dblValues(segment) = value.toDouble
			case _ =>
		}

		new ConcreteEvent(System.currentTimeMillis, iEvent.timestamp, strValues, dblValues)
	}
}

case class EventIngressPackage(domain: String, tenant: String, category: String, event: IngressEvent)

case class EventPackage(blade: Blade, event: Event)

case class IngressEvent(timestamp: Long, data: Map[String,JValue])

/*

{
	"domain": "test-domain",
	"tenant": "test-tenant",
	"category": "test-category",
	"event": {
		"timestamp": 239847239798237,
		"data": {
			"facility": "Facility-1",
			"kwh": 543.08
		}
	}
}

*/