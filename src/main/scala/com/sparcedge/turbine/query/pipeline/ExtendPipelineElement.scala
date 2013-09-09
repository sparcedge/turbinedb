package com.sparcedge.turbine.query.pipeline

import com.sparcedge.turbine.query._
import com.sparcedge.turbine.data.SegmentValueHolder

object ExtendPipelineElement {
    def apply(extend: Extend): ExtendPipelineElement = new ExtendPipelineElement(extend)
}

class ExtendPipelineElement(extend: Extend) extends QueryPipelineElement {
    val iExtend = extend.copy()
    val extendPlaceholder = iExtend.extendPlaceholder

    override def extendPlaceholders(): Iterable[SegmentValueHolder] = Vector(extendPlaceholder)

    def apply(segmentValues: Iterable[SegmentValueHolder]) { 
        segmentValues foreach { placeholder =>
            iExtend(placeholder)
        }
    }

    def evaluate() {
        if(iExtend.satisfied) {
            iExtend.evaluate()
        }
    }

    def reset() { /* Nothing to do */ }
}