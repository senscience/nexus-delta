package ai.senscience.nexus.delta.kernel.utils

import com.github.jknack.handlebars.{EscapingStrategy, Handlebars as JHandlebars}

import scala.jdk.CollectionConverters.*

object Handlebars {

  private val instance = new JHandlebars().`with`(EscapingStrategy.NOOP)

  def apply(templateText: String, attributes: (String, Any)*): String = apply(templateText, attributes.toMap)

  def apply(templateText: String, attributes: Map[String, Any]): String = {
    instance.compileInline(templateText).apply(attributes.asJava)
  }
}
