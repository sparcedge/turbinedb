package com.sparcege.turbine.query

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import com.sparcedge.turbine.query._

class MatchSpec extends WordSpec with MustMatchers {
	
	"All Numeric Matches" should {
		"evaluate to false when provided a string" in {
			val eqmatch = new EqualsNumericMatch("test", 5.1)
			val neqmtch = new NotEqualsNumericMatch("test", 5.1)
			val gtmatch = new GreaterThanNumericMatch("test", 5.1)
			val gtematch = new GreaterThanEqualNumericMatch("test", 5.1)
			val ltmatch = new LessThanNumericMatch("test", 5.1)
			val ltematch = new LessThanEqualNumericMatch("test", 5.1)
			eqmatch("happy") must be (false)
			neqmtch("happy") must be (false)
			gtmatch("happy") must be (false)
			gtematch("happy") must be (false)
			ltmatch("happy") must be (false)
			ltematch("happy") must be (false)
		}
	}

	"All String Matches" should {
		"evaluate to false when provided a number" in {
			val eqmatch = new EqualsStringMatch("test", "value")
			val neqmtch = new NotEqualsStringMatch("test", "value")
			val gtmatch = new GreaterThanStringMatch("test", "value")
			val gtematch = new GreaterThanEqualStringMatch("test", "value")
			val ltmatch = new LessThanStringMatch("test", "value")
			val ltematch = new LessThanEqualStringMatch("test", "value")
			eqmatch(5.1) must be (false)
			neqmtch(5.1) must be (false)
			gtmatch(5.1) must be (false)
			gtematch(5.1) must be (false)
			ltmatch(5.1) must be (false)
			ltematch(5.1) must be (false)
		}
	}

	"An EqualsMatch" when {
		"numeric type" should {
			"evaluate to true when provided the same number" in {
				val mtch = new EqualsNumericMatch("test", 5.1)
				mtch(5.1) must be (true)
			}
			"evalutate to false when provided a different number" in {
				val mtch = new EqualsNumericMatch("test", 5.1)
				mtch(5.2) must be (false)
			}
		}
		"string type" should {
			"evaluate to true when provided the same string" in {
				val mtch = new EqualsStringMatch("test", "happy")
				mtch("happy") must be (true)
			}
			"evalutate to false when provided a different string" in {
				val mtch = new EqualsStringMatch("test", "happy")
				mtch("not happy") must be (false)
			}
		}
	}

	"A NotEqualsMatch" when {
		"numeric type" should {
			"evaluate to true when provided a different number" in {
				val mtch = new NotEqualsNumericMatch("test", 5.1)
				mtch(5.2) must be (true)
			}
			"evalutate to false when provided the same number" in {
				val mtch = new NotEqualsNumericMatch("test", 5.1)
				mtch(5.1) must be (false)
			}
		}
		"string type" should {
			"evaluate to true when provided the a different string" in {
				val mtch = new NotEqualsStringMatch("test", "happy")
				mtch("so happy") must be (true)
			}
			"evalutate to false when provided the same string" in {
				val mtch = new NotEqualsStringMatch("test", "happy")
				mtch("happy") must be (false)
			}
		}
	}

	"A GreaterThanMatch" when {
		"numeric type" should {
			"evaluate to true when provided a greater value" in {
				val mtch = new GreaterThanNumericMatch("test", 5.1)
				mtch(5.2) must be (true)
			}
			"evaluate to false when provided a lesser value" in {
				val mtch = new GreaterThanNumericMatch("test", 5.1)
				mtch(5.0) must be (false)
			}
			"evaluate to false when provided the same value" in {
				val mtch = new GreaterThanNumericMatch("test", 5.1)
				mtch(5.1) must be (false)
			}
		}
		"string type" should {
			"evaluate to true when provided a greater value" in {
				val mtch = new GreaterThanStringMatch("test", "5.1")
				mtch("5.2") must be (true)
			}
			"evaluate to false when provided a lesser value" in {
				val mtch = new GreaterThanStringMatch("test", "5.1")
				mtch("5.0") must be (false)
			}
			"evaluate to false when provided the same value" in {
				val mtch = new GreaterThanStringMatch("test", "5.1")
				mtch("5.1") must be (false)
			}
		}
	}

	"A GreaterThanEqualsMatch" when {
		"numeric type" should {
			"evaluate to true when provided a greater value" in {
				val mtch = new GreaterThanEqualNumericMatch("test", 5.1)
				mtch(5.2) must be (true)
			}
			"evaluate to false when provided a lesser value" in {
				val mtch = new GreaterThanEqualNumericMatch("test", 5.1)
				mtch(5.0) must be (false)
			}
			"evaluate to true when provided the same value" in {
				val mtch = new GreaterThanEqualNumericMatch("test", 5.1)
				mtch(5.1) must be (true)
			}
		}
		"string type" should {
			"evaluate to true when provided a greater value" in {
				val mtch = new GreaterThanEqualStringMatch("test", "5.1")
				mtch("5.2") must be (true)
			}
			"evaluate to false when provided a lesser value" in {
				val mtch = new GreaterThanEqualStringMatch("test", "5.1")
				mtch("5.0") must be (false)
			}
			"evaluate to true when provided the same value" in {
				val mtch = new GreaterThanEqualStringMatch("test", "5.1")
				mtch("5.1") must be (true)
			}
		}
	}

	"A LessThanMatch" when {
		"numeric type" should {
			"evaluate to true when provided a lesser value" in {
				val mtch = new LessThanNumericMatch("test", 5.1)
				mtch(5.0) must be (true)
			}
			"evaluate to false when provided a greater value" in {
				val mtch = new LessThanNumericMatch("test", 5.1)
				mtch(5.2) must be (false)
			}
			"evaluate to false when provided the same value" in {
				val mtch = new LessThanNumericMatch("test", 5.1)
				mtch(5.1) must be (false)
			}
		}
		"string type" should {
			"evaluate to true when provided a lesser value" in {
				val mtch = new LessThanStringMatch("test", "5.1")
				mtch("5.0") must be (true)
			}
			"evaluate to false when provided a greater value" in {
				val mtch = new LessThanStringMatch("test", "5.1")
				mtch("5.2") must be (false)
			}
			"evaluate to false when provided the same value" in {
				val mtch = new LessThanStringMatch("test", "5.1")
				mtch("5.1") must be (false)
			}
		}
	}

	"A LessThanEqualMatch" when {
		"numeric type" should {
			"evaluate to true when provided a lesser value" in {
				val mtch = new LessThanEqualNumericMatch("test", 5.1)
				mtch(5.0) must be (true)
			}
			"evaluate to false when provided a lesser value" in {
				val mtch = new LessThanEqualNumericMatch("test", 5.1)
				mtch(5.2) must be (false)
			}
			"evaluate to true when provided the same value" in {
				val mtch = new LessThanEqualNumericMatch("test", 5.1)
				mtch(5.1) must be (true)
			}
		}
		"string type" should {
			"evaluate to true when provided a lesser value" in {
				val mtch = new LessThanEqualStringMatch("test", "5.1")
				mtch("5.0") must be (true)
			}
			"evaluate to false when provided a greater value" in {
				val mtch = new LessThanEqualStringMatch("test", "5.1")
				mtch("5.2") must be (false)
			}
			"evaluate to true when provided the same value" in {
				val mtch = new LessThanEqualStringMatch("test", "5.1")
				mtch("5.1") must be (true)
			}
		}
	}

	"An InMatch" when {
		"empty" should {
			"evaluate to false when provided any value" in {
				val mtch = new InMatch("test", List[Any]())
				mtch("5.1") must be (false)
				mtch(5.1) must be (false)
			}
		}
		"containing values" should {
			"evaluate to true when provided contained value" in {
				val mtch = new InMatch("test", List[Any]("happy", 5.1, "5.1"))
				mtch("5.1") must be (true)
				mtch(5.1) must be (true)
			}
			"evaluate to false when provided value not in list" in {
				val mtch = new InMatch("test", List[Any]("happy", 5.1, "5.1"))
				mtch("not happy") must be (false)
			}
		}
	}

	"A NotInMatch" when {
		"empty" should {
			"evaluate to true when provided any value" in {
				val mtch = new NotInMatch("test", List[Any]())
				mtch("5.1") must be (true)
				mtch(5.1) must be (true)
			}
		}
		"containing values" should {
			"evaluate to true when provided value not in list" in {
				val mtch = new NotInMatch("test", List[Any]("happy", 5.1, "5.1"))
				mtch("super happy") must be (true)
			}
			"evaluate to false when provided contained value" in {
				val mtch = new NotInMatch("test", List[Any]("happy", 5.1, "5.1"))
				mtch("5.1") must be (false)
				mtch(5.1) must be (false)
			}
		}
	}
}