package ai.senscience.nexus.delta

import com.apicatalog.jsonld.document.JsonDocument
import jakarta.json.JsonValue

package object rdf {

  type TitaniumDocument = JsonDocument

  val emptyDocument: JsonDocument = JsonDocument.of(JsonValue.EMPTY_JSON_OBJECT)

}
