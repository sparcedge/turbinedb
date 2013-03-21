package com.sparcedge.turbine.util

import java.util.{TreeMap,SortedMap,NavigableMap}
import collection.JavaConversions._

object WrappedTreeMap {
	def apply[K,V](sortedMap: SortedMap[K,V]): WrappedTreeMap[K,V] = {
		WrappedTreeMap[K,V](new TreeMap(sortedMap))
	}

	def apply[K,V](navigableMap: NavigableMap[K,V]): WrappedTreeMap[K,V] = {
		new WrappedTreeMap[K,V](navigableMap)
	}
}

// Revisit Generics
class WrappedTreeMap[K,V](treeMap: NavigableMap[K,V] = new TreeMap[K,V]()) {

	def get(key: K): Option[V] = {
		if(treeMap.containsKey(key)) {
			Some(treeMap.get(key))
		} else {
			None
		}
	}

	def getUnsafe(key: K): V = {
		treeMap.get(key)
	}

	def apply(key: K): V = {
		treeMap.get(key)
	}

	def update(key: K, value: V) {
		treeMap.put(key, value)
	}

	def containsKey(key: K): Boolean = {
		treeMap.containsKey(key)
	}

	def getOrElseUpdate(key: K, op: => V): V = {
		if(treeMap.containsKey(key)) {
			treeMap.get(key)
		} else {
			val value = op
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

	def headMap(key: K, incl: Boolean): WrappedTreeMap[K,V] = {
		WrappedTreeMap(treeMap.headMap(key,incl))
	}

	def tailMap(key: K): WrappedTreeMap[K,V] = {
		WrappedTreeMap(treeMap.tailMap(key))
	}

	def tailMap(key: K, incl: Boolean): WrappedTreeMap[K,V] = {
		WrappedTreeMap(treeMap.tailMap(key,incl))
	}

	def subMap(sKey: K, sIncl: Boolean, eKey: K, eIncl: Boolean): WrappedTreeMap[K,V] = {
		WrappedTreeMap(treeMap.subMap(sKey, sIncl, eKey, eIncl))
	}

	def size(): Int = {
		treeMap.size
	}
}