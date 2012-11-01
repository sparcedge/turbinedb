package com.sparcedge.turbine.blade.cache

import com.sparcedge.turbine.blade.mongo.MongoDBConnection
import com.mongodb.casbah.query.Imports._
import com.mongodb.casbah.MongoCursor
import akka.dispatch.ExecutionContext
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.blade.util.{Timer,WrappedTreeMap,BladeMetaData}
import org.joda.time.format.DateTimeFormat

object EventCache {

	def apply(blade: Blade)(implicit mongoConnection: MongoDBConnection): EventCache = {
		val diskCache = new DiskCache(blade)
		new EventCache(blade, diskCache)
	}

	def createCursor(blade: Blade, itsOpt: Option[Long])(implicit mongoConnection: MongoDBConnection): MongoCursor = {
		val collection = mongoConnection.collection
		var q: MongoDBObject = 
			("ts" $gte blade.periodStart.getMillis $lt blade.periodEnd.getMillis) ++ 
			("d" -> new ObjectId(blade.domain)) ++
			("t" -> new ObjectId(blade.tenant)) ++
			("c" -> blade.category)
		itsOpt.foreach(its => q ++= ("its" $gt its))
		val order: MongoDBObject = MongoDBObject("its" -> 1)
		val cursor = collection.find(q) //.sort(order)
		cursor.batchSize(mongoConnection.batchSize)
		cursor
	}
}

class EventCache(val blade: Blade, val diskCache: DiskCache) {
	val periodStart = blade.periodStart.getMillis
	val periodEnd = blade.periodEnd.getMillis
	val aggregateCache = new AggregateCache(this)

	// Currently only working with groupings / reducers
	def applyQuery(query: TurbineQuery)(implicit ec: ExecutionContext): String = {
		val timer = new Timer()
		var json = ""

		timer.start()
		val aggregateResults = calculateAggregateResultsFromCache(query.query)
		timer.stop("Query Processing")
		timer.start()
		json = CustomJsonSerializer.serializeAggregateGroupMap(aggregateResults)
		timer.stop("Serialize Results")

		json
	}

	def calculateAggregateResultsFromCache(query: Query)(implicit ec: ExecutionContext): WrappedTreeMap[String,List[ReducedResult]] = {
		aggregateCache.calculateQueryResults(query)
	}

	def update()(implicit mongoConnection: MongoDBConnection) {
		val cursor = EventCache.createCursor(blade, Some(diskCache.metaData.timestamp))
		diskCache.addEventsAndExecute(cursor) { event =>
			aggregateCache.updateCachedAggregates(event)
		}
	}
}