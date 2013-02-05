package com.sparcedge.turbine.query

import org.joda.time.format.DateTimeFormat

case class Blade(domain: String, tenant: String, category: String, period: String) {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")
	val periodStart = formatter.parseDateTime(period)
	val periodEnd = periodStart.plusMonths(1)
	override def toString() = domain + "." + tenant + "." + category + "." + period
}