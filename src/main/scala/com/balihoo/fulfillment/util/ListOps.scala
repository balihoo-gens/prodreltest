package com.balihoo.fulfillment.util
import scala.annotation.tailrec

object ListOps {
  /**
    * Generic function to iterate over a list using tail recursion
    *  calling the given function f on the head until Some is returned
    * @param l the iterable to iterate over
    * @param f the function to call on each element in l
    * @return Some(result) if calling f on the element returns in Some
    *         None if no element in the iterable results in Some after applying f to it
    */
  @tailrec
  def iterateUntilSome[T,U](l: Iterable[U], f: U => Option[T]): Option[T] = {
    if (l.isEmpty) {
      None
    } else {
      f(l.head) match {
        case Some(value) => Some(value)
        case None => iterateUntilSome[T,U](l.tail,f)
      }
    }
  }
}

