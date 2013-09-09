package com.sparcedge.turbine.query.pipeline

import com.sparcedge.turbine.Blade
import com.sparcedge.turbine.data.{SegmentValueHolder,Index,QueryUtil}
import com.sparcedge.turbine.query._

object ReducePipelineElement {
    def apply(groupings: Iterable[Grouping], indexes: Iterable[Index], blade: Blade) = new ReducePipelineElement(groupings, indexes, blade)
}

class ReducePipelineElement(groupings: Iterable[Grouping], indexes: Iterable[Index], blade: Blade) extends QueryPipelineElement {

    val indexGrouping = IndexGrouping("hour", blade.periodStartMS)
    val indexesArr = indexes.toArray
    val groupingArr = (List(indexGrouping) ++ groupings.map(_.copy)).toArray
    val groupingValues = new Array[String](groupingArr.length)

    override def apply(segmentValues: Iterable[SegmentValueHolder]) {
        segmentValues foreach { placeholder =>
            groupingArr foreach { grouping =>
                grouping(placeholder)
            }
            indexesArr foreach { index =>
                index(placeholder)
            }
        }
    }

    override def evaluate() {
        var idx = 0
        while(idx < groupingArr.length) {
            groupingValues(idx) = groupingArr(idx).evaluate()
            idx += 1
        }
        val groupStr = QueryUtil.createGroupString(groupingValues)

        idx = 0
        while (idx < indexesArr.length) {
            indexesArr(idx).evaluate(groupStr)
            idx += 1
        }
    }

    override def reset() { /* Do Nothing */ }
}