package com.sparcedge.turbine.event

import scala.util.{Try,Success,Failure}
import scala.collection.mutable
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsNumber,JsString}

import com.sparcedge.turbine.data.IndexKey
import com.sparcedge.turbine.query.{Blade,Collection}

object EventPackage {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")

	def fromEventIngressPackage(eiPackage: EventIngressPackage): EventPackage = {
		val event = convertIngressEventToEvent(eiPackage.event)
		val period = formatter.print(new DateTime(event.ts))
		EventPackage(Blade(Collection(eiPackage.domain, eiPackage.tenant, eiPackage.category), period), event)
	}

	def convertIngressEventToEvent(iEvent: IngressEvent): Event = {
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()

		iEvent.data.fields.foreach { 
			case (segment, JsString(value)) => strValues(segment) = value
			case (segment, JsNumber(value)) => dblValues(segment) = value.toDouble
			case _ =>
		}

		new Event(System.currentTimeMillis, iEvent.timestamp, strValues, dblValues)
	}
}

case class EventPackage(blade: Blade, event: Event)

object EventIngressPackage {
	implicit val iEventFormat = Json.format[IngressEvent]
	implicit val eiPackageFormat = Json.format[EventIngressPackage]
	val formatter = DateTimeFormat.forPattern("yyyy-MM")

	def tryParse(eventJson: String, domain: String, tenant: String, category: String): Try[EventIngressPackage] = {
		Try {
			val json = Json.parse(eventJson)
			val iEvent = json.as[IngressEvent]
			EventIngressPackage(domain, tenant, category, iEvent)
		}
	}

	def toBytes(eventPkg: EventIngressPackage): Array[Byte] = {
		Json.stringify(Json.toJson(eventPkg)).getBytes
	}

	def fromBytes(bytes: Array[Byte]): EventIngressPackage = {
		val json = Json.parse(bytes)
		json.as[EventIngressPackage]
	}
}

case class EventIngressPackage(domain: String, tenant: String, category: String, event: IngressEvent)

case class IngressEvent(timestamp: Long, data: JsObject)