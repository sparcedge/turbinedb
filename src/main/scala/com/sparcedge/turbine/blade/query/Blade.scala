package com.sparcedge.turbine.blade.query

case class Blade(domain: String, tenant: String, category: String, period: String) {
	val formatter = DateTimeFormat.forPattern("yyyy-MM")
	val periodStart = formatter.parseDateTime(period)
	val periodEnd = periodStart.plusMonths(1)
	val segmentCacheString = blade.domain + "." + blade.tenant + "." + blade.category + "." + blade.period
}