package ai.senscience.nexus.delta.testplugin

import ai.senscience.nexus.testkit.plugin.ClassLoaderTestClass

class ClassLoaderTestClassImpl extends ClassLoaderTestClass {

  def loadedFrom: String = "plugin classpath"
}
