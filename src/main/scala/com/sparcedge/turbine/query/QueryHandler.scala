package com.sparcedge.turbine.query

import scala.concurrent.{ExecutionContext,Await,Future}
import scala.concurrent.duration._
import scala.util.{Success,Failure}
import scala.async.Async.{async, await}

import akka.actor.{Actor,ActorRef}
import akka.util.Timeout
import akka.pattern.ask
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

	implicit val timeout = Timeout(240.seconds)
	implicit val ec: ExecutionContext = context.dispatcher 
	val monthFmt = DateTimeFormat.forPattern("yyyy-MM")

	var outCount = 0

	def receive = {
		case HandleQuery(queryPackage, ctx) => handleQuery(queryPackage, ctx)
		case _ =>
	}

	def handleQuery(queryPackage: TurbineQueryPackage, ctx: RequestContext) {
		val query = queryPackage.query
		val outMap = query.reducers.map(r => (r.reducer -> r.outputProperty)).toMap

		async {
			val bladeManagers = await(getBladeManagers(queryPackage))
			val indexManagers = await(getIndexManagers(bladeManagers, query))
			val indexes = await(getIndexes(indexManagers))
			val combinedIndex = sliceFlattenAndCombineIndexes(indexes, query, outMap)
			convertCombinedIndexToJson(combinedIndex)
		} onComplete {
			case Success(json) => 
				ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(json)))
			case Failure(err) => 
				err.printStackTrace()
				ctx.complete(HttpResponse(StatusCodes.InternalServerError))
		}
	}

	def getBladeManagers(queryPackage: TurbineQueryPackage): Future[Iterable[ActorRef]] = {
		val query = queryPackage.query
		val sPeriodOpt = query.start.map(monthFmt.print)
		val ePeriodOpt = query.end.map(monthFmt.print)
		val req = BladeManagerRangeRequest(queryPackage.collection, sPeriodOpt, ePeriodOpt)

		async {
			val response = await((bladeManagerRepository ? req).mapTo[BladeManagerRangeResponse])
			response.managers.map(_._2)
		}
	}

	def getIndexManagers(bladeManagers: Iterable[ActorRef], query: TurbineQuery): Future[Iterable[ActorRef]] = {
		async {
			val responseFutures = bladeManagers.map(_.ask(IndexesRequest(query)).mapTo[IndexesResponse])
			val responses = await(Future.sequence(responseFutures))
			responses.flatMap(_.indexes)
		}
	}

	def getIndexes(indexManagers: Iterable[ActorRef]): Future[Iterable[Index]] = {
		async {
			val responseFutures = indexManagers.map(_.ask(IndexRequest()).mapTo[IndexResponse])
			val responses = await(Future.sequence(responseFutures))
			responses.map(_.index)
		}
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
		var iGrouping = IndexGrouping("hour", monthStart)

		if(lowerBoundBroken) {
			sliced = sliced.tailMap(iGrouping.evaluate(query.start.get))
		}
		if(upperBoundBroken) {
			sliced = sliced.headMap(iGrouping.evaluate(query.end.get))
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