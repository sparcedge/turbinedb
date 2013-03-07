package com.sparcedge.turbine.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.{ExecutionContext,Await,Future}
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
			val bladeManagers = requestBladeManagers(queryPackage)
			val indexActors = requestIndexActors(bladeManagers, query)
			val indexes = retrieveIndexesFromActors(indexActors)
			val combinedIndex = sliceFlattenAndCombineIndexes(indexes, query, outMap)
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

	def requestBladeManagers(queryPackage: TurbineQueryPackage): Future[Iterable[ActorRef]] = {
		val query = queryPackage.query
		val sPeriodOpt = query.start.map(monthFmt.print)
		val ePeriodOpt = query.end.map(monthFmt.print)
		val req = BladeManagerRangeRequest(queryPackage.collection, sPeriodOpt, ePeriodOpt)

		(bladeManagerRepository ? req).mapTo[BladeManagerRangeResponse].map(_.managers.map(_._2))
	}

	def requestIndexActors(bladeManagers: Future[Iterable[ActorRef]], query: TurbineQuery): Future[Iterable[ActorRef]] = {
		val indexActorResponses: Future[Iterable[IndexesResponse]] = bladeManagers flatMap { managers =>
			Future.sequence (
				managers.map(manager => (manager ? IndexesRequest(query)).mapTo[IndexesResponse])
			)
		}
		indexActorResponses.map(responses => responses.flatMap(_.indexes))
	}

	def retrieveIndexesFromActors(indexActors: Future[Iterable[ActorRef]]): Future[Iterable[Index]] = {
		val indexResponses: Future[Iterable[IndexResponse]] = indexActors flatMap { actors => 
				Future.sequence ( 
					actors.map(actor => (actor ? IndexRequest()).mapTo[IndexResponse])
				)
			}
		indexResponses.map(responses => responses.map(res => res.index))
	}

	def sliceFlattenAndCombineIndexes(indexes: Future[Iterable[Index]], query: TurbineQuery, outMap: Map[Reducer,String]): Future[WrappedTreeMap[String,List[OutputResult]]] = {
		val sliced = indexes.map(idxs => idxs.map(idx => (idx -> sliceIndex(idx, query))))
		val flattened = sliced.map(idxs => idxs.map { case (idx, agg) => removeHourGroupFlattendAndReduceAggregate(agg, retrieveOutputParameter(idx, outMap)) })
		flattened.map(combineAggregates(_))
	}

	def retrieveOutputParameter(index: Index, outMap: Map[Reducer,String]): String = {
		outMap(index.indexKey.reducer)
	}

	def convertCombinedIndexToJson(combined: Future[WrappedTreeMap[String,List[OutputResult]]]): Future[String] = {
		combined.map(CustomJsonSerializer.serializeAggregateGroupMap(_))
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
		println(s"Aggregates Size: ${aggregates.size}")
		aggregates foreach { aggregate =>
			println(aggregate.size)
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