package ai.senscience.nexus.delta.sdk.directives

import ai.senscience.nexus.delta.rdf.jsonld.context.RemoteContextResolution
import ai.senscience.nexus.delta.rdf.utils.JsonKeyOrdering
import ai.senscience.nexus.delta.sdk.fusion.FusionConfig
import ai.senscience.nexus.delta.sdk.model.BaseUri

/**
  * Bundles the ambient, request-independent dependencies shared by (almost) every route: the base URI, the JSON-LD
  * rendering givens and the Fusion config. Per-module values (the [[org.typelevel.otel4s.trace.Tracer]] and the
  * pagination config) are deliberately kept out and passed separately, as they differ between modules.
  *
  * A route takes it as `(using ctx: RouteContext)` and exposes the givens with `import ctx.given`; the base URI is also
  * available directly as `ctx.baseUri`.
  */
final case class RouteContext(
    baseUri: BaseUri,
    rcr: RemoteContextResolution,
    ordering: JsonKeyOrdering,
    fusion: FusionConfig
) {
  given BaseUri                 = baseUri
  given RemoteContextResolution = rcr
  given JsonKeyOrdering         = ordering
  given FusionConfig            = fusion
}
