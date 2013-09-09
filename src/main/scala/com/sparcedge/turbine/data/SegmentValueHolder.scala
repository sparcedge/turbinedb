package com.sparcedge.turbine.data

import DataTypes._

object SegmentValueHolder {
    def apply(segment: String) = new SegmentValueHolder(segment)
}

class SegmentValueHolder(val segment: String) {
    private var dblValue: Double = 0.0
    private var lngValue: Long = 0L
    private var strValue: String = ""
    private var segType: Int = NIL

    def apply(num: Double) {
        dblValue = num
        segType = NUMBER
    }

    def apply(str: String) {
        strValue = str
        segType = STRING
    }

    def apply(lng: Long) {
        lngValue = lng
        segType = TIMESTAMP
    }

    def isDouble(): Boolean = segType == NUMBER

    def isString(): Boolean = segType == STRING

    def isTimestamp(): Boolean = segType == TIMESTAMP

    def isNil(): Boolean = segType == NIL 

    def getDouble(): Double = dblValue

    def getString(): String = strValue

    def getTimestamp(): Long = lngValue

    def getType(): Int = segType

    def clear() {
        segType = NIL
    }
}