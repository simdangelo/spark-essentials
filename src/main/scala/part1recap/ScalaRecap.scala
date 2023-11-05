package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello, Scala") // Unit = "no meaningful value" = void in other language

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companions
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // functional programming
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  // map. flatMap, filter
  val processedList = List(1, 2, 3).map(incrementer)

  // pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // future

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found the $meaningOfLife")
    case Failure(ex) => println(s"I have failed $ex")
  }

  // partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 2 => 56
    case _ => 999
  }

  // implicits
  // autoinjection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument(67)

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet // fromStringToPerson("Bob").greet


  // implicit conversion - implicit class
  implicit class Dog(name: String) {
    def bark = println("Bark")
  }
  "Lassie".bark

  /*
  - local scope
  - imported scope
  - companion object of the types involved in the method call
   */

}
