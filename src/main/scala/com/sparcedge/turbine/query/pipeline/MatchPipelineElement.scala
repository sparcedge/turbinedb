package com.sparcedge.turbine.query.pipeline

import com.sparcedge.turbine.query._
import com.sparcedge.turbine.data.SegmentValueHolder

object MatchPipelineElement {
    def apply(mtch: Match): MatchPipelineElement = new MatchPipelineElement(mtch)
}

class MatchPipelineElement(mtch: Match) extends QueryPipelineElement {
    val iMatch = mtch.copy()
    var satisfied = false
    val segment = iMatch.segment

    override def shouldContinue(): Boolean = satisfied

    def apply(segmentValues: Iterable[SegmentValueHolder]) { 
        segmentValues foreach { placeholder =>
            iMatch(placeholder)
        }
    }

    def evaluate() {
        satisfied = iMatch.evaluate()
    }

    def reset() { 
        satisfied = false
    }
}