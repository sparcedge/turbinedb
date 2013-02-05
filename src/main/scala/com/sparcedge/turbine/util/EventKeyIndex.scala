package com.sparcedge.turbine.util

import scala.collection.mutable

class EventKeyIndex(val indexMap: mutable.Map[Short,String] = mutable.Map[Short,String]()) {
	val valueMap = mutable.Map[String,Short]()
	private var greatest = Short.MinValue.toInt

	indexMap foreach { case (key, value) =>
		valueMap(value) = key
		if(key > greatest) {
			greatest = key.toInt
		}
	}

	var curr = greatest

	def getIndexValueAndOptionallyAdd(key: String): Short = {
		if(valueMap.contains(key)) {
			valueMap(key)
		} else {
			val index = curr.toShort
			indexMap(index) = key
			valueMap(key) = index
			curr += 1
			index
		}
	}

	def get(index: Short): String = {
		indexMap(index)
	}
}