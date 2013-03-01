package com.sparcedge.turbine

import org.joda.time.format.DateTimeFormat

case class Blade(collection: Collection, period: String) {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")
	val periodStart = formatter.parseDateTime(period)
	val periodStartMS = periodStart.getMillis()
	val periodEnd = periodStart.plusMonths(1)
	val key = toString()
	override def toString() = collection.domain + "." + collection.tenant + "." + collection.category + "." + period
}

case class Collection(domain: String, tenant: String, category: String)