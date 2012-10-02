package com.sparcedge.turbine.blade.util

import java.util.{TreeMap,SortedMap}

object WrappedTreeMap {

	def apply[K,V](sortedMap: SortedMap[K,V]): WrappedTreeMap[K,V] = {
		val wrappedTreeMap = new WrappedTreeMap[K,V]()
		wrappedTreeMap.putAll(sortedMap)
		wrappedTreeMap
	}
}

// Revisit Generics
class WrappedTreeMap[K,V] extends TreeMap[K,V] {

	def apply(key: K): V = {
		get(key)
	}

	def update(key: K, value: V) {
		put(key, value)
	}

	def getOrElseUpdate(key: K, value: V): V = {
		if(containsKey(key)) {
			get(key)
		} else {
			put(key, value)
			value
		}
	}

	def foreach(fun: ((K,V)) => Unit) {
		val iterator = entrySet.iterator
		while(iterator.hasNext) {
			val entry = iterator.next
			fun(entry.getKey -> entry.getValue)
		}
	}
}