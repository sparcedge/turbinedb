package com.sparcedge.turbine.blade.util

object Timer {
	var printTimings = false
}

class Timer {
	var sTime = 0L

	def start() {
		sTime = System.currentTimeMillis
	}

	def stop(msg: String) {
		stop(msg, 0)
	}

	def stop(msg: String, nesting: Int) {
		val runTime = System.currentTimeMillis - sTime
		if(Timer.printTimings) {
			println(("  " * nesting) + msg + ": " + runTime)
		}
	}
}