package com.sparcedge.turbine.blade.query.cache

import scala.collection.immutable.TreeMap
import scala.collection.GenMap
import net.liftweb.json._
import com.sparcedge.turbine.blade.query._

object CustomJsonSerializer {
	implicit val formats = Serialization.formats(NoTypeHints)

	def serializeAggregateGroupMap(aggregateMap: TreeMap[String,Iterable[ReducedResult]]): String = {		
		serializeGroupMap(aggregateMap, serializeReducedResults, serializeReducedResultsMeta)
	}

	def serializeAggregateGroupMap(aggregateMap: GenMap[String,Iterable[ReducedResult]]): String = {		
		serializeGroupMap(TreeMap(aggregateMap.toArray:_*), serializeReducedResults, serializeReducedResultsMeta)
	}

	def serializeEventGroupMap(eventGroupMap: TreeMap[String,Iterable[Event]]): String = {
		serializeGroupMap(eventGroupMap, serializeEvents, serializeEventsMeta)
	}

	def serializeGroupMap[T<:Any](groupMap: TreeMap[String,Iterable[T]], valueSerializer: (Iterable[T]) => String, metaSerializer: (Iterable[T]) => String): String = {
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
		key.split(QueryResolver.GROUP_SEPARATOR)
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

	private def serializeEventsMeta(events: Iterable[Event]): String = {
		"" // No meta for raw events
	}

	private def serializeEvents(events: Iterable[Event]): String = {
		Serialization.write(events)
	}
}