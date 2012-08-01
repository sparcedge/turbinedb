package com.sparcedge.turbine.blade.query.cache

import scala.collection.mutable
import scala.collection.immutable.TreeMap
import com.sparcedge.turbine.blade.query._

object QueryResolver {

	val GROUP_SEPARATOR = "✈"
	val GROUP_SEPARATOR_CHAR = '✈'

	def applyGroupings(events: Iterable[Event], groupings: Iterable[Grouping]): TreeMap[String,Iterable[Event]] = {
		val eventGroupings = mutable.Map[String,List[Event]]()
		events.map(applyGroupingsToEvent(_, groupings)).foreach { case (groupStr, event) =>
			val groupedEvents = eventGroupings.getOrElseUpdate(groupStr, List[Event]())
			eventGroupings += (groupStr -> (event :: groupedEvents))
		}
		TreeMap(eventGroupings.toArray:_*)
	}

	def applyGroupingsToEvent(event: Event, groupings: Iterable[Grouping]): (String,Event) = {
		val groupStr = groupings.map(_.groupFunction(event)).mkString(GROUP_SEPARATOR)
		(groupStr,event)
	}

	def applyReducersToEventGroupings(eventGroupings: TreeMap[String, Iterable[Event]], reducers: Iterable[Reducer]): TreeMap[String, Iterable[ReducedResult]] = {
		eventGroupings.map { case (key, value) => (key, applyReducers(value, reducers)) }
	}

	def applyReducers(events: Iterable[Event], reducers: Iterable[Reducer]): Iterable[ReducedResult] = {
		reducers.map { reducer =>
			val result = reducer.reduceFunction(events)
			result.output = Some(reducer.propertyName)
			result
		}
	}
	
	def applyReducerToEventGroupings(eventGroupings: TreeMap[String, Iterable[Event]], reducer: Reducer): TreeMap[String, ReducedResult] = {
		eventGroupings.map { case (key, value) => (key, applyReducer(value, reducer)) }
	}

	def applyReducer(events: Iterable[Event], reducer: Reducer): ReducedResult = {
		reducer.reduceFunction(events)
	}

	def applyMatches(events: Iterable[Event], matchLst: Iterable[Match]): Iterable[Event] = {
		matchLst match {
			case Nil =>
				events
			case matches =>
				events filter { event =>
					matches forall { _(event) }
				}
		}
	}

	def removeHourGroupFlattendAndReduceAggregate(aggregate: TreeMap[String,ReducedResult], output: String): TreeMap[String,ReducedResult] = {
		var flattened = mutable.Map[String,List[ReducedResult]]()
		aggregate foreach { case (key,value) =>
			val newKey = key.dropWhile(_ != GROUP_SEPARATOR_CHAR).tail
			val results = flattened.getOrElseUpdate(newKey, List[ReducedResult]())
			flattened +=  (newKey -> (value :: results))
		}
		val reduced = flattened.mapValues { results => 
			val result = Reduce.reReduce(results)
			result.output = Some(output)
			result
		}

		TreeMap(reduced.toArray:_*)
	}

	def flattenAggregates(aggregates: List[TreeMap[String,ReducedResult]]): TreeMap[String,Iterable[ReducedResult]] = {
		var flattened = mutable.Map[String,List[ReducedResult]]()
		aggregates foreach { aggregate =>
			aggregate foreach { case (key,value) =>
				val results = flattened.getOrElseUpdate(key, List[ReducedResult]())
				flattened += (key -> (value :: results))
			}
		}
		TreeMap(flattened.toArray:_*)
	}
}