package com.sparcege.turbine.query

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import com.sparcedge.turbine.query._

class MaxReducerSpec extends WordSpec with MustMatchers {
	val maxReducer = MaxReducer("test")
	"A MaxReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = maxReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = maxReducer.createReducedResult()
				val emptyRes2 = maxReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct maximum" in {
				val result = maxReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				assert(result.getResultValue == nums.max)
			}
			"produce the correct value after reReducing" in {
				val result1 = maxReducer.createReducedResult()
				val result2 = maxReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				assert(result1.getResultValue == (nums1 ++ nums2).max)
			}
		}
	}
}

class MinReducerSpec extends WordSpec {
	val minReducer = MinReducer("test")
	"A MaxReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = minReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = minReducer.createReducedResult()
				val emptyRes2 = minReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct minimum" in {
				val result = minReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				assert(result.getResultValue == nums.min)
			}
			"produce the correct value after reReducing" in {
				val result1 = minReducer.createReducedResult()
				val result2 = minReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				assert(result1.getResultValue == (nums1 ++ nums2).min)
			}
		}
	}
}

class AvgReducerSpec extends WordSpec {
	val avgReducer = AvgReducer("test")
	"A AvgReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = avgReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = avgReducer.createReducedResult()
				val emptyRes2 = avgReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct average" in {
				val result = avgReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				val avg = nums.sum / nums.size
				assert(result.getResultValue == avg)
			}
			"produce the correct value after reReducing" in {
				val result1 = avgReducer.createReducedResult()
				val result2 = avgReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				val cmbd = (nums1 ++ nums2)
				val avg = cmbd.sum / cmbd.size
				assert(result1.getResultValue == avg)
			}
		}
	}
}

class CountReducerSpec extends WordSpec {
	val countReducer = CountReducer("test")
	"A CountReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = countReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = countReducer.createReducedResult()
				val emptyRes2 = countReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct count" in {
				val result = countReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				assert(result.getResultValue == nums.size)
			}
			"correctly count strings" in {
				val result = countReducer.createReducedResult()
				val strs = List("one","two","three","four","five")
				strs.foreach(result(_))
				assert(result.getResultValue == strs.size)
			}
			"produce the correct value after reReducing" in {
				val result1 = countReducer.createReducedResult()
				val result2 = countReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				assert(result1.getResultValue == (nums1 ++ nums2).size)
			}
		}
	}
}

class SumReducerSpec extends WordSpec {
	val sumReducer = SumReducer("test")
	"A SumReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = sumReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = sumReducer.createReducedResult()
				val emptyRes2 = sumReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct sum" in {
				val result = sumReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				assert(result.getResultValue == nums.sum)
			}
			"produce the correct value after reReducing" in {
				val result1 = sumReducer.createReducedResult()
				val result2 = sumReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				assert(result1.getResultValue == (nums1 ++ nums2).sum)
			}
		}
	}
}

class StDevReducerSpec extends WordSpec {
	val stdReducer = StDevReducer("test")
	"A StDevReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = stdReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = stdReducer.createReducedResult()
				val emptyRes2 = stdReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct standard deviation" in {
				val result = stdReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				// TODO: Implement
			}
		}
		"produce the correct value after reReducing" in {
				val result1 = stdReducer.createReducedResult()
				val result2 = stdReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				// TODO: Implement
			}
	}
}

class RangeReducerSpec extends WordSpec {
	val rangeReducer = RangeReducer("test")
	"A RangeReducedResult" when {
		"empty" should {
			"have a value of 0" in {
				val emptyResult = rangeReducer.createReducedResult()
				assert(emptyResult.getResultValue == 0.0)
			}
			"reReduce with another empty result and still have a value of 0" in {
				val emptyRes1 = rangeReducer.createReducedResult()
				val emptyRes2 = rangeReducer.createReducedResult()
				emptyRes1.reReduce(emptyRes2)
				assert(emptyRes1.getResultValue == 0.0)
			}
		}
		"supplied with values" should {
			"calculate the correct range" in {
				val result = rangeReducer.createReducedResult()
				val nums = List[Double](1,2,3,4,5,6,7,8,9,10)
				nums.foreach(result(_))
				assert(result.getResultValue == (nums.max - nums.min))
			}
		}
		"produce the correct value after reReducing" in {
				val result1 = rangeReducer.createReducedResult()
				val result2 = rangeReducer.createReducedResult()
				val nums1 = List[Double](1,2,3,4,5)
				val nums2 = List[Double](6,7,8,9,10)
				nums1.foreach(result1(_))
				nums2.foreach(result2(_))
				result1.reReduce(result2)
				assert(result1.getResultValue == ((nums1 ++ nums2).max) - ((nums1 ++ nums2).min))
			}
	}
}