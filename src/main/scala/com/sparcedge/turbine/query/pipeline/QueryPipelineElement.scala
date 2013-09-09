package com.sparcedge.turbine.query.pipeline

import collection.JavaConverters._
import java.util.HashMap
import com.sparcedge.turbine.data.SegmentValueHolder

abstract class QueryPipelineElement {
    def shouldContinue: Boolean = true
    def extendPlaceholders: Iterable[SegmentValueHolder] = Vector[SegmentValueHolder]()

    def reset()
    def evaluate()
    def apply(segmentValues: Iterable[SegmentValueHolder])
}