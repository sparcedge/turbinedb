package com.sparcedge.turbine.blade.util

import java.util.{TreeMap,SortedMap}
import collection.JavaConversions._

object WrappedTreeMap {
	def apply[K,V](sortedMap: SortedMap[K,V]): WrappedTreeMap[K,V] = {
		new WrappedTreeMap[K,V](sortedMap)
	}
}

// Revisit Generics
class WrappedTreeMap[K,V](treeMap: SortedMap[K,V] = new TreeMap[K,V]()) {

	def apply(key: K): V = {
		treeMap.get(key)
	}

	def update(key: K, value: V) {
		treeMap.put(key, value)
	}

	def containsKey(key: K): Boolean = {
		treeMap.containsKey(key)
	}

	def getOrElseUpdate(key: K, value: V): V = {
		if(treeMap.containsKey(key)) {
			treeMap.get(key)
		} else {
			treeMap.put(key, value)
			value
		}
	}

	def foreach(fun: ((K,V)) => Unit) {
		val iterator = treeMap.entrySet.iterator
		while(iterator.hasNext) {
			val entry = iterator.next
			fun(entry.getKey -> entry.getValue)
		}
	}

	def values(): Iterable[V] = {
		treeMap.values
	}

	def headMap(key: K): WrappedTreeMap[K,V] = {
		WrappedTreeMap(treeMap.headMap(key))
	}

	def tailMap(key: K): WrappedTreeMap[K,V] = {
		WrappedTreeMap(treeMap.tailMap(key))
	}

	def size(): Int = {
		treeMap.size
	}
}