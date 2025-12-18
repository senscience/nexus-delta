package ai.senscience.nexus.delta.elasticsearch.configured

import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import io.circe.Json

final case class ConfiguredIndexDocument(types: Set[Iri], value: Json)
