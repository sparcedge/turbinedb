package com.sparcedge.turbine.blade.cache

import scala.collection.immutable.TreeMap
import com.mongodb.casbah.query.Imports._
import akka.dispatch.ExecutionContext
import com.sparcedge.turbine.blade.query._
import com.sparcedge.turbine.util.BEFUtil

object EventCache {

	def apply(blade: Blade)(implicit mongoConnection: MongoDBConnection): EventCache = {
		val cursor = createCursor(blade, None)
		try {
			BEFUtil.ensureCacheFileExists(blade)
			val newestTimestamp = BEFUtil.serializeAndAddEvents(cursor, blade)
		} finally {
			corsor.close()
		}
		new EventCache(blade, newestTimestamp)
	}

	def createCursor(blade: Blade, its: Option[Long])(implicit mongoConnection: MongoDBConnection): MongoCursor = {
		val collection = mongoConnection.collection
		var q: MongoDBObject = 
			("ts" $gte blade.periodStart.getMillis $lt blade.periodEnd.getMillis) ++ 
			("d" -> new ObjectId(blade.domain)) ++
			("t" -> new ObjectId(blade.tenant)) ++
			("c" -> blade.category)
		its.foreach(q ++ ("its" $gt _))
		val order: MongoDBObject = MongoDBObject("its" -> 1)
		val cursor = collection.find(q).sort(order)
		cursor.batchSize(mongoConnection.batchSize)
		cursor
	}
}

// TODO: Update Cache Functionality
class EventCache(val blade: Blade, var newestTimestamp: Long) {
	val aggregateCache = new AggregateCache(this)

	// Currently only working with groupings / reducers
	def applyQuery(query: TurbineQuery)(implicit ec: ExecutionContext): String = {
		val timer = new Timer()
		var json = ""

		timer.start()
		val aggregateResults = calculateAggregateResultsFromCache(query.query)
		var endTime = System.currentTimeMillis
		timer.stop("Query Processing")
		timer.start()
		json = CustomJsonSerializer.serializeAggregateGroupMap(aggregateResults)
		timer.stop("Serialize Results")

		json
	}

	def calculateAggregateResultsFromCache(query: Query)(implicit ec: ExecutionContext): TreeMap[String,Iterable[ReducedResult]] = {
		aggregateCache.calculateQueryResults(query)
	}

	def update() {
		val cursor = EventCache.createCursor(blade, Some(newestTimestamp))
		// Apply Update to Event Cache
		// Apply Update to Aggregate Caches
	}
}
