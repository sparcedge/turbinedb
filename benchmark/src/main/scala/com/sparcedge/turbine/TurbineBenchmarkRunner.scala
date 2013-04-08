package com.sparcedge.turbine

import com.google.caliper.{Runner => CaliperRunner}

import com.sparcedge.turbine.data.GroupStringBuilderBenchmark

object TurbineBenchmarkRunner {

	def main(args: Array[String]) {
		CaliperRunner.main(classOf[GroupStringBuilderBenchmark], args)
	}
}