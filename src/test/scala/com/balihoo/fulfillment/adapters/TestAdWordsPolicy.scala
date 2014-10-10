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
      AdWordsPolicy.cleanUrl("balihoo.com") mustEqual "http://balihoo.com"
      try {
        AdWordsPolicy.cleanUrl("notbalihoo.com") mustEqual "http://balihoo.com"
        failure("Expected an exception cause that URL does not exist")
      } catch {
        case e: Exception =>
          e.getMessage mustEqual "URL:notbalihoo.com does NOT resolve!"
      }

      true
    }

    "  fix upper case violations" in {
      AdWordsPolicy.fixUpperCaseViolations("there ARE some wORds WIth vioLATIONS") mustEqual "there Are some Words With Violations"
    }

    "  match domains" in {
      try {
        AdWordsPolicy.matchDomains("randomdomainname.net/stork/angle/possum/spine", "https://randomdomainname.net/")
      } catch {
        case e:Exception =>
          failure("That should have worked! "+e.getMessage)
      }

      success
    }

    "  NOT match domains" in {
      try {
        AdWordsPolicy.matchDomains("http://superspecificname.net/stork/angle/possum/spine", "https://randomdomainname.net/")
        failure("That should NOT have worked! ")
      } catch {
        case e:Exception =>
          e.getMessage mustEqual "Domains for destination and display URLs must match! (randomdomainname.net =/= superspecificname.net)"
      }

      true

    }

    "  validate keyword" in {
      try {
        AdWordsPolicy.validateKeyword("this is a valid keyword")
        success
      } catch {
        case e:Exception =>
          failure("This should be valid! "+e.getMessage)
      }
    }

    "  NOT validate too many word keyword" in {
      try {
        AdWordsPolicy.validateKeyword("this is NOT a valid keyword because it has more than 10 words")
        failure("That should have been too long!")
      } catch {
        case e:Exception =>
          e.getMessage mustEqual "Keyword 'this is NOT a valid keyword because it has more than 10 words' has too many words! (max 10)"
      }
      true
    }

    "  NOT validate too long keyword" in {
      try {
        AdWordsPolicy.validateKeyword("this isNOTavalidkeywordbecauseithasmorethan80characters whichistoomanyyeahitsureis")
        failure("That should have been too long!")
      } catch {
        case e:Exception =>
          e.getMessage mustEqual "Keyword 'this isNOTavalidkeywordbecauseithasmorethan80characters whichistoomanyyeahitsureis' is too long! (max 80)"
      }
      true
    }
  }

}
