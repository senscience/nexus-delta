package ai.senscience.nexus.delta.rdf.jsonld.api

import ai.senscience.nexus.delta.rdf.jsonld.api.JsonLdApiConfig.ErrorHandling

/**
  * Configuration for the json
  *
  * @param strict
  *   set the Jena parser to strict mode
  * @param extraChecks
  *   set the Jena parser to perform additional checks
  * @param errorHandling
  *   set the error handler fpr the jena parser
  */
final case class JsonLdApiConfig(strict: Boolean, extraChecks: Boolean, errorHandling: ErrorHandling)

object JsonLdApiConfig {

  enum ErrorHandling {

    /**
      * Keep the default error handler, errors will throw exception but only log warnings
      */
    case Default

    /**
      * Error handler that will throw exceptions for both errors and warnings
      */
    case Strict

    /**
      * Error handler that will throw exceptions for errors but do nothing about warnings
      */
    case NoWarning
  }
}
