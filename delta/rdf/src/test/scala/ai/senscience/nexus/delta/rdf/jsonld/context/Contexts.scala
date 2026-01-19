package ai.senscience.nexus.delta.rdf.jsonld.context

import ai.senscience.nexus.testkit.CirceLiteral

object Contexts extends CirceLiteral {

  val value: ContextValue =
    JsonLdContext.topContextValueOrEmpty(
      json"""
           {
            "@context": {
              "id": "@id",
              "@vocab": "http://senscience.ai/",
              "@base": "http://nexus.senscience.ai/",
              "xsd": "http://www.w3.org/2001/XMLSchema#",
              "Person": "https://schema.org/Person",
              "schema": "https://schema.org/",
              "deprecated": {
                "@id": "https://schema.org/deprecated",
                "@type": "xsd:boolean"
              },
              "customid": {
                "@type": "@id"
              }
            }
          }
          """
    )

}
