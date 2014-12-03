package com.balihoo.fulfillment.util

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

@RunWith(classOf[JUnitRunner])
class TestHostIdentify extends Specification with Mockito
{
  "Test Host Identity" should {
    "  get a host name " in {
      HostIdentity.getHostAddress.size must beGreaterThan(0)
    }

    "  find the 3rd value " in {
      val result = ListOps.iterateUntilSome(
        List("", "", "third", "fourth"),
        (s:String) => if (s.isEmpty) None else Some(s)
      )
      result.get must beEqualTo("third")
    }
  }
}


