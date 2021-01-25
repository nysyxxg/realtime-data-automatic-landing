package com.data.auto.landing.common.jdk7

import org.slf4j.LoggerFactory

import scala.language.implicitConversions

class ScalaAutoCloseable[T <: AutoCloseable](autoCloseable: T) {

  private val log = LoggerFactory.getLogger(getClass)

  def use[R](func: T => R): R = {
    try {
      func.apply(autoCloseable)
    } catch {
      case throwable: Throwable =>
        log.error(throwable.getMessage, throwable)
        throw throwable
    } finally {
      if (autoCloseable != null) {
        log.debug(s"${autoCloseable.getClass} close.")
        autoCloseable.close()
      } else {
        log.warn("autoCloseable is null.")
      }
    }
  }
}

object ScalaAutoCloseable {
  implicit def wrapAsScalaAutoCloseable[T <: AutoCloseable](autoCloseable: T): ScalaAutoCloseable[T] = {
    new ScalaAutoCloseable[T](autoCloseable)
  }
}
