package com.sparcedge.turbine.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import akka.dataflow._
import scala.concurrent.{ExecutionContext,Await,Future}
import scala.concurrent.duration._
import scala.util.{Try,Success,Failure}
import scala.collection.mutable
import akka.util.Timeout
import akka.pattern.ask
import spray.routing.RequestContext
import spray.http.{HttpResponse,HttpEntity,StatusCodes}

import com.sparcedge.turbine.util.{WrappedTreeMap,CustomJsonSerializer}
import com.sparcedge.turbine.data._

object QueryHandler {

	case class HandleQuery(query: TurbineQuery, bladeManager: ActorRef, ctx: RequestContext)

}

import AggregateIndex._
import BladeManager._
import QueryHandler._

class QueryHandler extends Actor {

	implicit val timeout = Timeout(240 seconds)
	implicit val ec: ExecutionContext = context.dispatcher 

	var outCount = 0

	def receive = {
		case HandleQuery(tquery, bladeMan, ctx) =>
			val query = tquery.query

			// In the future!!!
			val indexActors = requestIndexActors(bladeMan, tquery)
			val indexes = retrieveIndexesFromActors(indexActors)
			val combinedIndex = sliceFlattenAndCombineIndexes(indexes, query)
			val jsonResult = convertCombinedIndexToJson(combinedIndex)

			jsonResult.onComplete {
				case Success(json) => 
					ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(json)))
				case Failure(err) => 
					err.printStackTrace()
					ctx.complete(HttpResponse(StatusCodes.InternalServerError))
			}
		case _ =>
	}

	def requestIndexActors(bladeManager: ActorRef, query: TurbineQuery): Future[Iterable[ActorRef]] = {
		(bladeManager ? IndexesRequest(query)).mapTo[IndexesResponse].map(_.indexes)
	}

	def retrieveIndexesFromActors(indexActors: Future[Iterable[ActorRef]]): Future[Iterable[Index]] = {
		val indexResponses: Future[Iterable[IndexResponse]] = indexActors flatMap { actors => 
				Future.sequence ( 
					actors.map (actor => (actor ? IndexRequest()).mapTo[IndexResponse])
				)
			}
		indexResponses.map(responses => responses.map(res => res.index))
	}

	def sliceFlattenAndCombineIndexes(indexes: Future[Iterable[Index]], query: Query): Future[WrappedTreeMap[String,List[ReducedResult]]] = {
		val sliced = indexes.map(idxs => idxs.map(sliceIndex(_, query)))
		val flattened = sliced.map(idxs => idxs.map(removeHourGroupFlattendAndReduceAggregate(_, s"out-${nextCount()}")))
		flattened.map(combineAggregates(_))
	}

	def convertCombinedIndexToJson(combined: Future[WrappedTreeMap[String,List[ReducedResult]]]): Future[String] = {
		combined.map(CustomJsonSerializer.serializeAggregateGroupMap(_))
	}

	// TODO: DIRTY DIRTY HACK
	def nextCount(): Int = {
		outCount += 1
		outCount
	}

	private def sliceIndex(indexVal: Index, query: Query): WrappedTreeMap[String,ReducedResult] = {
		var sliced = indexVal.index
		val lowerBoundBroken = query.range.start > indexVal.blade.periodStart.getMillis
		var upperBoundBroken = query.range.end != None && query.range.end.get < indexVal.blade.periodEnd.getMillis

		if(lowerBoundBroken) {
			sliced = sliced.tailMap(query.startPlusMinute)
		}
		if(upperBoundBroken) {
			sliced = sliced.headMap(query.endMinute.get)
		}

		sliced
	}

	def removeHourGroupFlattendAndReduceAggregate(aggregate: WrappedTreeMap[String,ReducedResult], output: String): WrappedTreeMap[String,ReducedResult] = {
		var flattenedReduced = new WrappedTreeMap[String,ReducedResult]()
		aggregate foreach { case (key,value) =>
			try {
				val newKey = key.substring(QueryUtil.GROUPING_LENGTH)
				if(flattenedReduced.containsKey(newKey)) {
					flattenedReduced(newKey) = flattenedReduced(newKey).reReduce(value)
				} else {
					flattenedReduced(newKey) = value.createOutputResult(output)
				}
			} catch {
				case ex: StringIndexOutOfBoundsException => // TODO: Handle
			}
		}

		flattenedReduced
	}

	def combineAggregates(aggregates: Iterable[WrappedTreeMap[String,ReducedResult]]): WrappedTreeMap[String,List[ReducedResult]] = {
		var combined = new WrappedTreeMap[String,List[ReducedResult]]()
		aggregates foreach { aggregate =>
			aggregate foreach { case (key,value) =>
				val results = combined.getOrElseUpdate(key, List[ReducedResult]())
				combined(key) = (value :: results)
			}
		}
		combined
	}
}