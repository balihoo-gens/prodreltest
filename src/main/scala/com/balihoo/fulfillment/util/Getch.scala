package com.balihoo.fulfillment.util
import scala.tools.jline.console.ConsoleReader
import scala.concurrent.{Future, future, blocking}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Class to monitor and store keyboard input
 *   asynchronously using a future
 * The advantage over regular getch is that all characters are
 *   read and stored without the need for polling.
 */
class Getch {
  private var _keysPressed: String = ""
  private var _go = false
  private val _con = new ConsoleReader
  private var _future: Option[Future[Unit]] = None
  private val _fmap = collection.mutable.Map[Seq[String], () => Unit]()

  def start: Unit = {
    _go = true
    _future = Some(_createFuture)
    //when the future completes, reset the var
    _future map { _ => _future = None }
  }

  def stop: Unit = {
    _go = false
  }

  def clear: Unit = {
    _keysPressed = ""
  }

  def _createFuture: Future[Unit] = {
    future { blocking {
      while (_go) {
        if (_con.getInput.available > 0) {
          _keysPressed += _con.readVirtualKey.toChar
          for ((strings, function) <- _fmap ) {
            if (caughtOneOf(strings)) {
              function()
            }
          }
        } else {
          Thread.sleep(10)
        }
      }
    }}
  }

  def addMapping(strings: Seq[String], function: () => Unit): Unit = {
    _fmap(strings) = function
  }

  def caughtOneOf(strings: Seq[String]): Boolean = {
    strings.foldLeft(false)( (a,b) =>
      a || (_keysPressed.contains(b)))
  }

  def doWith[T](code: => T): T = {
    start
    val res = code
    stop
    clear
    res
  }
}

/**
 * Companion object for the above class.
 * Simple non-blocking key reading function
 *   returning the character value of a key entered
 */
object Getch {
  private val _con = new ConsoleReader

  def getch = {
    if (_con.getInput.available > 0) {
      _con.readVirtualKey.toChar
    }
  }
}

/**
 * Simple tests for the above class and companion object
 */
object testGetch extends App {
  val g = new Getch

  println("start static getch")
  while (!(Getch.getch == 'q')) {
    Thread.sleep(100)
    print(".")
  }
  println("done")

  println("start basic doWith")
  g.doWith {
    while (!g.caughtOneOf(Seq("q", "Q", "Exit"))) {
      Thread.sleep(100)
      print(".")
    }
  }
  println("done")

  println("start map with")
  var done = false
  g.addMapping(Seq("e", "E", "Quit"), () => done = true )
  g.addMapping(Seq("p", "Print"), () =>  println("Wheeee!"))
  g.addMapping(Seq("c", "Clear"), () =>  g.clear)
  g.doWith {
    while (!done) {
      Thread.sleep(100)
      print(".")
    }
  }
  println("done")
}
