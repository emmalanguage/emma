package eu.stratosphere.emma

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir.{FoldSink, TempSink, ValueRef, Write}

import scala.reflect.runtime.{universe => ru}


package object runtime {

  abstract class Engine {

    def execute[A](root: FoldSink[A]): ValueRef[A]

    def execute[A](root: TempSink[A]): ValueRef[DataBag[A]]

    def execute[A](root: Write[A]): Unit

    def scatter[A](values: Seq[A]): ValueRef[DataBag[A]]

    def gather[A](ref: ValueRef[DataBag[A]]): DataBag[A]

    def put[A](value: A): ValueRef[A]

    def get[A](ref: ValueRef[A]): A

    def closeSession(): Unit
  }

  case object Native extends Engine {

    def execute[A](root: FoldSink[A]): ValueRef[A] = ???

    def execute[A](root: TempSink[A]): ValueRef[DataBag[A]] = ???

    def execute[A](root: Write[A]): Unit = ???

    def scatter[A](values: Seq[A]): ValueRef[DataBag[A]] = ???

    def gather[A](ref: ValueRef[DataBag[A]]): DataBag[A] = ???

    def put[A](value: A): ValueRef[A] = ???

    def get[A](ref: ValueRef[A]): A = ???

    def closeSession(): Unit = ???
  }

  def factory(name: String, host: String, port: Int) = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    // reflect engine
    val engineClazz = mirror.staticClass(s"${getClass.getPackage.getName}.${name.capitalize}")
    val engineClazzMirror = mirror.reflectClass(engineClazz)
    val engineClassType = ru.appliedType(engineClazz)
    // reflect engine constructor
    val constructorMirror = engineClazzMirror.reflectConstructor(engineClassType.decl(ru.termNames.CONSTRUCTOR).asMethod)
    // instantiate engine
    constructorMirror(host, port).asInstanceOf[Engine]
  }
}
