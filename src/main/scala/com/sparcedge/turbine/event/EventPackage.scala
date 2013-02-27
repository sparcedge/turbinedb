package com.sparcedge.turbine.event

import scala.util.{Try,Success,Failure}
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

import com.sparcedge.turbine.data.IndexKey
import com.sparcedge.turbine.query.Blade

object EventPackage {
	implicit val formats = org.json4s.DefaultFormats
	val formatter = DateTimeFormat.forPattern("yyyy-MM")

	def fromEventIngressPackage(eiPackage: EventIngressPackage): EventPackage = {
		val event = convertIngressEventToEvent(eiPackage.event)
		val period = formatter.print(new DateTime(event.ts))
		EventPackage(Blade(eiPackage.domain, eiPackage.tenant, eiPackage.category, period), event)
	}

	def convertIngressEventToEvent(iEvent: IngressEvent): Event = {
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()

		iEvent.data.obj.foreach { 
			case (segment, JString(value)) => strValues(segment) = value
			case (segment, JDouble(value)) => dblValues(segment) = value
			case (segment, JInt(value)) => dblValues(segment) = value.toDouble
			case (segment, JDecimal(value)) => dblValues(segment) = value.toDouble
			case _ =>
		}

		new Event(System.currentTimeMillis, iEvent.timestamp, strValues, dblValues)
	}
}

case class EventPackage(blade: Blade, event: Event)

object EventIngressPackage {
	implicit val formats = org.json4s.DefaultFormats
	val formatter = DateTimeFormat.forPattern("yyyy-MM")

	def tryParse(eventJson: String): Try[EventIngressPackage] = {
		Try {
			val json = parse(eventJson)
			json.extract[EventIngressPackage]
		}
	}

	def toBytes(eventPkg: EventIngressPackage): Array[Byte] = {
		write(eventPkg).getBytes
	}

	def fromBytes(bytes: Array[Byte]): EventIngressPackage = {
		val json = parse(new String(bytes))
		json.extract[EventIngressPackage]
	}
}

case class EventIngressPackage(domain: String, tenant: String, category: String, event: IngressEvent)

case class IngressEvent(timestamp: Long, data: JObject)