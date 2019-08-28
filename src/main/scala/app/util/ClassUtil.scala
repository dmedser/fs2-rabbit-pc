package app.util

import scala.reflect.runtime.universe.{WeakTypeTag, weakTypeOf}

object ClassUtil {
  def nameOf[T : WeakTypeTag]: String =
    weakTypeOf[T].typeSymbol.name.decodedName.toString
}
