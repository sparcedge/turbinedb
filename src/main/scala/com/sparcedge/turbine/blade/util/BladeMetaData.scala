package com.sparcedge.turbine.blade.util


// TODO: Potentially make EventKeyIndex immutable
class BladeMetaData (
	val timestamp: Long = 0L, 
	val eventKeyIndex: EventKeyIndex = new EventKeyIndex
)
