package com.sparcedge.turbine.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.{ExecutionContext,Await,Future,future}
import scala.concurrent.duration._
import scala.util.{Try,Success,Failure}
import scala.collection.mutable
import spray.routing.RequestContext
import spray.http.{HttpResponse,HttpEntity,StatusCodes}
import org.joda.time.format.DateTimeFormat

import com.sparcedge.turbine.BladeManagerRepository
import com.sparcedge.turbine.util.{WrappedTreeMap,CustomJsonSerializer}
import com.sparcedge.turbine.data._

object QueryHandler {
	case class HandleQuery(extQuery: TurbineQueryPackage, ctx: RequestContext)
}

import AggregateIndex._
import BladeManager._
import QueryHandler._
import BladeManagerRepository._
import QueryUtil._

class QueryHandler(bladeManagerRepository: ActorRef) extends Actor {

	implicit val timeout = Timeout(240 seconds)
	implicit val ec: ExecutionContext = context.dispatcher 
	val monthFmt = DateTimeFormat.forPattern("yyyy-MM")

	var outCount = 0

	def receive = {
		case HandleQuery(queryPackage, ctx) =>
			val query = queryPackage.query

			val outMap = query.reducers.map(r => (r.reducer -> r.outputProperty)).toMap

			// In the future!!!
			val jsonResult = for (
				bladeManagers <- getBladeManagers(queryPackage);
				indexManagers <- getIndexManagers(bladeManagers, query);
				indexes <- getIndexes(indexManagers);
				combinedIndex <- future { sliceFlattenAndCombineIndexes(indexes, query, outMap) };
				json <- future { convertCombinedIndexToJson(combinedIndex) }
			) yield json

			jsonResult.onComplete {
				case Success(json) => 
					ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(json)))
				case Failure(err) => 
					err.printStackTrace()
					ctx.complete(HttpResponse(StatusCodes.InternalServerError))
			}
		case _ =>
	}

	def getBladeManagers(queryPackage: TurbineQueryPackage): Future[Iterable[ActorRef]] = {
		val query = queryPackage.query
		val sPeriodOpt = query.start.map(monthFmt.print)
		val ePeriodOpt = query.end.map(monthFmt.print)
		val req = BladeManagerRangeRequest(queryPackage.collection, sPeriodOpt, ePeriodOpt)

		(bladeManagerRepository ? req).mapTo[BladeManagerRangeResponse].map(_.managers.map(_._2))
	}

	def getIndexManagers(bladeManagers: Iterable[ActorRef], query: TurbineQuery): Future[Iterable[ActorRef]] = {
		val indexManagerResponses = Future.sequence (
			bladeManagers map { manager => (manager ? IndexesRequest(query)).mapTo[IndexesResponse] }
		)
		indexManagerResponses.map(responses => responses.flatMap(_.indexes))
	}

	def getIndexes(indexManagers: Iterable[ActorRef]): Future[Iterable[Index]] = {
		val indexResponses = Future.sequence (
			indexManagers map { manager => (manager ? IndexRequest()).mapTo[IndexResponse] }
		)
		indexResponses.map(responses => responses.map(res => res.index))
	}

	def sliceFlattenAndCombineIndexes(indexes: Iterable[Index], query: TurbineQuery, outMap: Map[Reducer,String]): WrappedTreeMap[String,List[OutputResult]] = {
		val sliced = indexes.map(idx => (idx -> sliceIndex(idx, query)))
		val flattened = sliced map { case (idx, agg) => removeHourGroupFlattendAndReduceAggregate(agg, retrieveOutputParameter(idx, outMap)) }
		combineAggregates(flattened)
	}

	def retrieveOutputParameter(index: Index, outMap: Map[Reducer,String]): String = {
		outMap(index.indexKey.reducer)
	}

	def convertCombinedIndexToJson(combined: WrappedTreeMap[String,List[OutputResult]]): String = {
		CustomJsonSerializer.serializeAggregateGroupMap(combined)
	}

	private def sliceIndex(indexVal: Index, query: TurbineQuery): WrappedTreeMap[String,ReducedResult] = {
		var sliced = indexVal.index
		val monthStart = indexVal.blade.periodStart.getMillis
		val lowerBoundBroken = query.start != None && query.start.get > monthStart
		var upperBoundBroken = query.end != None && query.end.get < indexVal.blade.periodEnd.getMillis

		if(lowerBoundBroken) {
			sliced = sliced.tailMap(DATA_GROUPING(query.start.get, monthStart))
		}
		if(upperBoundBroken) {
			sliced = sliced.headMap(DATA_GROUPING(query.end.get, monthStart))
		}

		sliced
	}

	def removeHourGroupFlattendAndReduceAggregate(aggregate: WrappedTreeMap[String,ReducedResult], output: String): WrappedTreeMap[String,OutputResult] = {
		var flattenedReduced = new WrappedTreeMap[String,OutputResult]()
		aggregate foreach { case (key,value) =>
			val newKey = key.substring(QueryUtil.GROUPING_LENGTH)
			if(flattenedReduced.containsKey(newKey)) {
				flattenedReduced(newKey).reReduce(value)
			} else {
				flattenedReduced(newKey) = value.copyForOutput(output)
			}
		}

		flattenedReduced
	}

	def combineAggregates(aggregates: Iterable[WrappedTreeMap[String,OutputResult]]): WrappedTreeMap[String,List[OutputResult]] = {
		var combined = new WrappedTreeMap[String,List[OutputResult]]()
		aggregates foreach { aggregate =>
			aggregate foreach { case (key,value) =>
				val results = combined.getOrElseUpdate(key, List[OutputResult]())
				val resOpt = results.find(r => r.segment == value.segment && r.reduceType == value.reduceType)
				if(resOpt.isDefined) {
					resOpt.get.reReduce(value)
				} else {
					combined(key) = (value :: results)
				}
			}
		}
		combined
	}
}