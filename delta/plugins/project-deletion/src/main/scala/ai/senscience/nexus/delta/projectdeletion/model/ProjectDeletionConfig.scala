package ai.senscience.nexus.delta.projectdeletion.model

import ai.senscience.nexus.delta.rdf.jsonld.context.ContextValue
import ai.senscience.nexus.delta.rdf.jsonld.context.JsonLdContext.keywords
import ai.senscience.nexus.delta.rdf.jsonld.encoder.JsonLdEncoder
import ai.senscience.nexus.delta.sdk.marshalling.HttpResponseFields
import io.circe.{Encoder, Json, JsonObject}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

/**
  * Automatic Project Deletion configuration.
  *
  * @param idleInterval
  *   the interval after which a project is considered idle
  * @param idleCheckPeriod
  *   how often to check for idle projects
  * @param deleteDeprecatedProjects
  *   whether to delete deprecated projects immediately, without waiting for them to become idle
  * @param includedProjects
  *   a list of regexes that select which projects to be included in the idle check for automatic deletion
  * @param excludedProjects
  *   a list of regexes that select which projects to be excluded from the idle check for automatic deletion
  */
final case class ProjectDeletionConfig(
    idleInterval: FiniteDuration,
    idleCheckPeriod: FiniteDuration,
    deleteDeprecatedProjects: Boolean,
    includedProjects: List[Regex],
    excludedProjects: List[Regex]
)

object ProjectDeletionConfig {

  given JsonLdEncoder[ProjectDeletionConfig] = {
    given Encoder.AsObject[ProjectDeletionConfig] =
      Encoder.encodeJsonObject.contramapObject { cfg =>
        JsonObject(
          keywords.tpe                -> Json.fromString("ProjectDeletionConfig"),
          "_idleIntervalInSeconds"    -> Json.fromLong(cfg.idleInterval.toSeconds),
          "_idleCheckPeriodInSeconds" -> Json.fromLong(cfg.idleCheckPeriod.toSeconds),
          "_deleteDeprecatedProjects" -> Json.fromBoolean(cfg.deleteDeprecatedProjects),
          "_includedProjects"         -> Json.arr(cfg.includedProjects.map(str => Json.fromString(str.regex))*),
          "_excludedProjects"         -> Json.arr(cfg.excludedProjects.map(str => Json.fromString(str.regex))*)
        )
      }

    JsonLdEncoder.computeFromCirce(ContextValue(contexts.projectDeletion))
  }

  given HttpResponseFields[ProjectDeletionConfig] = HttpResponseFields.defaultOk

  given ConfigReader[ProjectDeletionConfig] =
    deriveReader[ProjectDeletionConfig].emap { cfg =>
      Either.cond(
        cfg.idleInterval.toMillis > cfg.idleCheckPeriod.toMillis,
        cfg,
        CannotConvert(
          cfg.idleCheckPeriod.toString,
          classOf[FiniteDuration].getSimpleName,
          "'idle-interval' cannot be smaller than 'idle-check-period'"
        )
      )
    }
}
