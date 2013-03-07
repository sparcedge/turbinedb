package com.sparcedge.turbine.behaviors

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import akka.actor.{Actor,Cancellable}

object BatchBehavior {
	case object Flush
}

import BatchBehavior._

trait BatchBehavior { this: Actor =>

	val maxBatchSize: Int
	val maxTimeUnflushed: Int
	var scheduledFlush: Option[Cancellable] = None
	var batchSize: Int = 0

	def batchReceive: Receive = {
		case Flush => flush()
	}	

	def incrementBatchSize() {
		batchSize += 1
		if(batchSize >= maxBatchSize) {
			flush()
		} else if(scheduledFlush == None) {
			scheduleFlush()
		}
	}

	def flush() {
		scheduledFlush.foreach(_.cancel)
		scheduledFlush = None
		batchSize = 0
		flushBatch()
	}

	def scheduleFlush() {
		scheduledFlush = Some(context.system.scheduler.scheduleOnce(maxTimeUnflushed milliseconds, self, Flush))
	}

	def flushBatch()
}

trait BatchStorage[T] extends BatchBehavior { this: Actor =>
	val	 batch = mutable.ArrayBuffer[T]()

	override def flush() {
		super.flush()
		clearBatch()
	}

	def addToBatch(value: T) {
		batch += value
		incrementBatchSize()
	}

	def clearBatch() {
		batch.clear()
	}

	def flushBatch() = flushBatch(batch)

	def flushBatch(batch: Iterable[T])
}