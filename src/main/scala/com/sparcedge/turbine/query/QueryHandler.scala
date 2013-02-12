package com.sparcedge.turbine.query

import java.io.{StringWriter,PrintWriter}
import akka.actor.{Actor,ActorRef}
import akka.dataflow._
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

			// In the future!!!
			val bladeManagers = requestBladeManagers(queryPackage)
			val indexActors = requestIndexActors(bladeManagers, query)
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

	def requestBladeManagers(queryPackage: TurbineQueryPackage): Future[Iterable[ActorRef]] = {
		val query = queryPackage.query
		val sPeriod = monthFmt.print(query.range.start)
		val ePeriodOpt = query.range.end.map(monthFmt.print)
		val sBlade = Blade(queryPackage.domain, queryPackage.tenant, queryPackage.category, sPeriod)
		
		var msg = if(ePeriodOpt.isDefined) {
			BladeManagerRangeRequest(sBlade, sBlade.copy(period = ePeriodOpt.get))
		} else {
			BladeManagerRangeUnboundedRequest(sBlade)
		}

		(bladeManagerRepository ? msg).mapTo[BladeManagerRangeResponse].map(_.managers.map(_._2))
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

	def sliceFlattenAndCombineIndexes(indexes: Future[Iterable[Index]], query: TurbineQuery): Future[WrappedTreeMap[String,List[ReducedResult]]] = {
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

	private def sliceIndex(indexVal: Index, query: TurbineQuery): WrappedTreeMap[String,ReducedResult] = {
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