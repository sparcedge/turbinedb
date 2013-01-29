package com.sparcedge.turbine.blade.util

import scala.collection.GenMap
import com.sparcedge.turbine.blade.data.QueryUtil
import com.sparcedge.turbine.blade.query.ReducedResult

object CustomJsonSerializer {

	def serializeAggregateGroupMap(aggregateMap: WrappedTreeMap[String,List[ReducedResult]]): String = {		
		serializeGroupMap(aggregateMap, serializeReducedResults, serializeReducedResultsMeta)
	}

	def serializeGroupMap[T<:Any](groupMap: WrappedTreeMap[String,List[T]], valueSerializer: (Iterable[T]) => String, metaSerializer: (Iterable[T]) => String): String = {
		var prevElements = Array[String]()
		val jsonBuilder = new StringBuilder
		
		jsonBuilder.append("[")
		
		groupMap foreach { case (key, values) =>
			val currElements = tokenizeKey(key)
			addJsonElementDiff(prevElements, currElements, jsonBuilder)
			jsonBuilder.append("{\"data\":[{")
			jsonBuilder.append(valueSerializer(values))
			jsonBuilder.append("}],\"meta\":{")
			jsonBuilder.append(metaSerializer(values))
			jsonBuilder.append("}}")
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

	private def serializeReducedResultsMeta(reducedResults: Iterable[ReducedResult]): String = {
		reducedResults.map(result => "\"" + result.output.get + "-count\":" + result.count).mkString(",")
	}

	private def serializeReducedResults(reducedResults: Iterable[ReducedResult]): String = {
		reducedResults.map(result => "\"" + result.output.get + "\":" + result.value).mkString(",")
	}
}