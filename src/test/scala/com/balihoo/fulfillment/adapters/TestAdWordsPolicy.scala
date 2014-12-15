package com.balihoo.fulfillment.adapters

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

// This class contains the tests.
@RunWith(classOf[JUnitRunner])
class TestAdWordsPolicy extends Specification with Mockito
{

  // Here come the tests.
  "AdWordsPolicy" should {
    "  strip leading 'www.'" in {
      AdWordsPolicy.noWWW("www.monkey.com") mustEqual "monkey.com"
    }
    "  not strip leading non'www.'" in {
      AdWordsPolicy.noWWW("uwww.monkey.com") mustEqual "uwww.monkey.com"
    }

    "  escape spaces" in {
      AdWordsPolicy.escapeSpaces("bucket of stork ankles") mustEqual "bucket%20of%20stork%20ankles"
    }

    "  remove protocol" in {
      AdWordsPolicy.noProtocol("https://cellardoor.com") mustEqual "cellardoor.com"
      AdWordsPolicy.noProtocol("http://cellardoor.com") mustEqual "cellardoor.com"
      AdWordsPolicy.noProtocol("ttp://cellardoor.com") mustEqual "ttp://cellardoor.com"
    }

    "  add protocol" in {
      AdWordsPolicy.addProtocol("https://cellardoor.com") mustEqual "http://cellardoor.com"
      AdWordsPolicy.addProtocol("http://cellardoor.com") mustEqual "http://cellardoor.com"
      AdWordsPolicy.addProtocol("hts://cellardoor.com") mustEqual "http://hts://cellardoor.com"
    }

    "  confirm url is good" in {
      AdWordsPolicy.cleanUrl("google.com") mustEqual "http://google.com"
      AdWordsPolicy.cleanUrl("notbalihoo.com") must throwA[Exception].like {
        case e => e.getMessage must contain("URL:notbalihoo.com does NOT resolve!")}
    }

    "  fix upper case violations" in {
      AdWordsPolicy.fixUpperCaseViolations("there ARE some wORds WIth vioLATIONS") mustEqual "there Are some Words With Violations"
    }

    "  match domains" in {
      AdWordsPolicy.matchDomains("randomdomainname.net/stork/angle/possum/spine", "https://randomdomainname.net/")
    }

    "  NOT match domains" in {
      AdWordsPolicy.matchDomains("http://superspecificname.net/stork/angle/possum/spine", "https://randomdomainname.net/") must throwA[Exception].like {
        case e => e.getMessage must contain("Domains for destination and display URLs must match! (randomdomainname.net =/= superspecificname.net)")}
    }

    "  validate keyword" in {
      AdWordsPolicy.validateKeyword("this is a valid keyword") == "this is a valid keyword"
    }

    "  NOT validate too many word keyword" in {
      AdWordsPolicy.validateKeyword("this is NOT a valid keyword because it has more than 10 words") must throwA[Exception].like {
        case e => e.getMessage must contain("Keyword 'this is NOT a valid keyword because it has more than 10 words' has too many words! (max 10)")}
    }

    "  NOT validate too long keyword" in {
      AdWordsPolicy.validateKeyword("this isNOTavalidkeywordbecauseithasmorethan80characters whichistoomanyyeahitsureis") must throwA[Exception].like {
        case e => e.getMessage must contain("Keyword 'this isNOTavalidkeywordbecauseithasmorethan80characters whichistoomanyyeahitsureis' is too long! (max 80)")}
    }
  }

}
