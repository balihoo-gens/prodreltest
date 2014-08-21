package com.balihoo.fulfillment.config

/**
 * This class performs validation on SWF identifiers.  An exception is thrown if the identifier is too long, or if it
 * contains illegal substrings.
 * @param ident the identifier string
 * @param maxLength An exception is thrown if ident exceeds this length.
 */
abstract class SWFIdentifier(ident: String, maxLength: Int) {

  for(s <- Array("##", ":", "/", "|", "arn")) {
    if(ident.contains(s)) throw new IllegalArgumentException(s"$ident must not contain '$s'")
  }

  if(ident.length > maxLength) throw new IllegalArgumentException(s"$ident must not be longer than '$maxLength'")

  override def toString() = ident
}

object SWFIdentifier {

  /**
   * This implicit conversion allows instances to be used as arguments to SWF API calls that expect strings.
   * @param identifier
   * @return
   */
  implicit def identifierToString(identifier: SWFIdentifier): String = identifier.toString

}

class SWFName(ident: String) extends SWFIdentifier(ident, 256)

class SWFVersion(ident:String) extends SWFIdentifier(ident, 64)
