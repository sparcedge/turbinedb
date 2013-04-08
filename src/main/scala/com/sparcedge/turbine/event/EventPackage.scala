package com.sparcedge.turbine.event

import scala.util.{Try,Success,Failure}
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import play.api.libs.json.Json
import play.api.libs.json.{JsObject,JsNumber,JsString}

import com.sparcedge.turbine.data.IndexKey
import com.sparcedge.turbine.{Blade,Collection}

object EventPackage {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")

	def fromEventIngressPackage(eiPackage: EventIngressPackage): EventPackage = {
		val event = convertIngressEventToEvent(eiPackage.event)
		val period = formatter.print(new DateTime(event.ts))
		EventPackage(Blade(eiPackage.collection, period), event)
	}

	def convertIngressEventToEvent(iEvent: IngressEvent): Event = {
		var strValues = Map[String,String]()
		var dblValues = Map[String,Double]()

		iEvent.data.fields.foreach { 
			case (segment, JsString(value)) => strValues += (segment -> value)
			case (segment, JsNumber(value)) => dblValues += (segment -> value.toDouble)
			case _ =>
		}

		new Event(System.currentTimeMillis, iEvent.timestamp, strValues, dblValues)
	}
}

case class EventPackage(blade: Blade, event: Event)

object IngressEvent {
	implicit val iEventFormat = Json.format[IngressEvent]

	def tryParse(eventJson: String): Try[IngressEvent] = {
		Try {
			val json = Json.parse(eventJson)
			json.as[IngressEvent]
		}
	}
}

case class IngressEvent(timestamp: Long, data: JsObject)

object EventIngressPackage {
	implicit val iEventFormat = Json.format[IngressEvent]
	implicit val collectionFormat = Json.format[Collection]
	implicit val eiPackageFormat = Json.format[EventIngressPackage]	

	def toBytes(eventPkg: EventIngressPackage): Array[Byte] = {
		Json.stringify(Json.toJson(eventPkg)).getBytes
	}

	def fromBytes(bytes: Array[Byte]): EventIngressPackage = {
		val json = Json.parse(bytes)
		json.as[EventIngressPackage]
	}
}

case class EventIngressPackage(collection: Collection, event: IngressEvent)