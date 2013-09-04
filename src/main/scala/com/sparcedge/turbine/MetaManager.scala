package com.sparcedge.turbine

import akka.actor.{Actor,Props,ActorSystem,ActorRef,ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import spray.routing.RequestContext
import spray.http.{HttpResponse,HttpEntity,StatusCodes}
import play.api.libs.json.Json
import scala.util.{Try,Success,Failure}
import scala.concurrent.{ExecutionContext,Future}
import scala.concurrent.duration._

import com.sparcedge.turbine.data.BladeManager

object MetaManager {
    case class DescribeDatabases(ctx: RequestContext)
    case class DescribeCollections(database: String, ctx: RequestContext)
    case class DescribeSegments(collection: Collection, ctx: RequestContext)
}

import MetaManager._
import BladeManagerRepository._
import BladeManager._

class MetaManager(bladeManagerRepository: ActorRef) extends Actor with ActorLogging {
    
    implicit val timeout = Timeout(240.seconds)
    implicit val ec: ExecutionContext = context.dispatcher

    def receive = {
        case DescribeDatabases(ctx: RequestContext) =>
            retrieveAndRespondWithDatabases(ctx)
        case DescribeCollections(database: String, ctx: RequestContext) =>
            println(s"Describing Collections for ${database}")
            retrieveAndRespondWithCollections(database, ctx)
        case DescribeSegments(collection: Collection, ctx: RequestContext) =>
            println(s"Describing Segments for ${collection}")
            retrieveAndRespondWithSegments(collection, ctx)
        case _ =>
    }

    def retrieveAndRespondWithDatabases(ctx: RequestContext) {
        (bladeManagerRepository ? DatabasesRequest()).mapTo[DatabasesResponse].onComplete {
            case Success(DatabasesResponse(databases)) =>
                ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(toJsonArray(databases))))
            case Failure(err) =>
                err.printStackTrace()
                ctx.complete(HttpResponse(StatusCodes.InternalServerError))
        }
    }

    def retrieveAndRespondWithCollections(database: String, ctx: RequestContext) {
        (bladeManagerRepository ? CollectionsRequest(database)).mapTo[CollectionsResponse].onComplete {
            case Success(CollectionsResponse(collections)) =>
                ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(toJsonArray(collections.map(_.collection)))))
            case Failure(err) =>
                err.printStackTrace()
                ctx.complete(HttpResponse(StatusCodes.InternalServerError))
        }
    }

    def retrieveAndRespondWithSegments(collection: Collection, ctx: RequestContext) {
        (bladeManagerRepository ? BladeManagerRangeRequest(collection)).mapTo[BladeManagerRangeResponse].flatMap { response =>
            Future.sequence {
                response.managers.map { case (_, manager) =>
                    (manager ? SegmentsRequest()).mapTo[SegmentsResponse].map(_.segments.toList)
                }
            }.map(_.toList.flatten.distinct)
        }.onComplete {
            case Success(segments) =>
                ctx.complete(HttpResponse(StatusCodes.OK, HttpEntity(toJsonArray(segments))))
            case Failure(err) =>
                err.printStackTrace()
                ctx.complete(HttpResponse(StatusCodes.InternalServerError))
        }
    }

    def toJsonArray(elements: Iterable[String]): String = {
        Json.stringify(Json.toJson(elements))
    }
}