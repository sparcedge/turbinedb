package com.sparcedge.turbine.util

import scala.collection.GenMap
import com.sparcedge.turbine.data.QueryUtil
import com.sparcedge.turbine.query.OutputResult

object CustomJsonSerializer {

	def serializeAggregateGroupMap(aggregateMap: WrappedTreeMap[String,List[OutputResult]]): String = {		
		serializeGroupMap(aggregateMap, serializeReducedResults)
	}

	def serializeGroupMap[T<:Any](groupMap: WrappedTreeMap[String,List[T]], valueSerializer: (Iterable[T]) => String): String = {
		var prevElements = Array[String]()
		val jsonBuilder = new StringBuilder
		
		jsonBuilder.append("[")
		
		groupMap foreach { case (key, values) =>
			val currElements = tokenizeKey(key)
			addJsonElementDiff(prevElements, currElements, jsonBuilder)
			jsonBuilder.append("{\"data\":[{")
			jsonBuilder.append(valueSerializer(values))
			jsonBuilder.append("}]}")
			prevElements = currElements
		}

		for(x <- 1 to prevElements.size) {
			jsonBuilder.append("]}")
		}

		jsonBuilder.append("]")
		jsonBuilder.toString
	}

	private def tokenizeKey(key: String): Array[String] = {
		key.split(QueryUtil.GROUP_SEPARATOR)
	}

	private def addJsonElementDiff(prev: Array[String], curr: Array[String], builder: StringBuilder) {
		var different = false
		for(i <- 0 to (curr.size - 1)) {
			if(different) {
				builder.append("{\"group\":\"").append(curr(i)).append("\"").append(",\"data\":[")
			} else if(prev.size <= i) { 
				different = true
				builder.append("{\"group\":\"").append(curr(i)).append("\"").append(",\"data\":[")
			} else if(prev(i) != curr(i)) {
				different = true
				val diffElems = prev.size - i
				for(x <- 1 to diffElems) {
					builder.append("]}")
				}
				builder.append(",").append("{\"group\":\"").append(curr(i)).append("\"").append(",\"data\":[")
			}
		}
	}

	private def serializeReducedResults(reducedResults: Iterable[OutputResult]): String = {
		reducedResults.map(result => "\"" + result.output + "\":" + result.getResultValue).mkString(",")
	}
}