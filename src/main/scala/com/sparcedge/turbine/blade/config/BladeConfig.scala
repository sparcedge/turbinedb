package com.sparcedge.turbine.blade.config

import com.sparcedge.turbine.blade.query.Blade
import org.json4s._
import org.json4s.jackson.JsonMethods._

object BladeConfig {

	implicit val formats = org.json4s.DefaultFormats

	def apply(confJson: String): BladeConfig = {
		val json = parse(confJson)
		json.extract[BladeConfig]
	}
}

case class BladeConfig (
	preloadBlades: Option[List[Blade]] = Some(List[Blade]()),
	dataDirectory: Option[String] = Some("data"),
	printTimings: Option[Boolean] = Some(false)
)

/*{
	"dataDirectory": "data",
	"preloadBlades": [
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"},
		{"domain": "", "tenant": "", "category": "", "period": "YYYYMM"}
	]
	"printTimings": true
}*/