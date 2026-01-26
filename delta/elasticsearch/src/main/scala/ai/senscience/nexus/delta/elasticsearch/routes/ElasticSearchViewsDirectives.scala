package ai.senscience.nexus.delta.elasticsearch.routes

import ai.senscience.nexus.delta.elasticsearch.client.ElasticSearchRequest
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.TypeOperator.Or
import ai.senscience.nexus.delta.elasticsearch.model.ResourcesSearchParams.{KeywordsParam, LogParam, Type, TypeOperator, TypeParams, VersionParams}
import ai.senscience.nexus.delta.rdf.IriOrBNode.Iri
import ai.senscience.nexus.delta.sdk.directives.UriDirectives
import ai.senscience.nexus.delta.sdk.marshalling.QueryParamsUnmarshalling.IriBase
import ai.senscience.nexus.delta.sdk.model.BaseUri
import ai.senscience.nexus.delta.sdk.model.search.{Sort, SortList}
import ai.senscience.nexus.delta.sdk.projects.model.ProjectContext
import ai.senscience.nexus.delta.sourcing.model.Identity.Subject
import ai.senscience.nexus.delta.sourcing.model.{Label, ResourceRef}
import ai.senscience.nexus.pekko.marshalling.CirceUnmarshalling.jsonObjectEntity
import io.circe.parser.parse
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.{Directive, Directive0, Directive1, MalformedQueryParamRejection}
import org.apache.pekko.http.scaladsl.unmarshalling.{FromStringUnmarshaller, Unmarshaller}

trait ElasticSearchViewsDirectives extends UriDirectives {

  private val simultaneousSortAndQRejection =
    MalformedQueryParamRejection("sort", "'sort' and 'q' parameters cannot be present simultaneously.")

  private val searchParamsSortAndPaginationKeys =
    Set("deprecated", "id", "rev", "from", "size", "after", "type", "schema", "createdBy", "updatedBy", "sort", "q")

  /**
    * Matches only if the ''aggregations'' parameter is set to ''true''
    */
  val aggregated: Directive0 =
    parameter("aggregations".as[Boolean].?).flatMap {
      case Some(true) => pass
      case _          => reject
    }

  /**
    * Extract the ''sort'' query parameter(s) and provide a [[SortList]]
    */
  val sortList: Directive1[SortList] = {
    given FromStringUnmarshaller[Sort] = Unmarshaller.strict[String, Sort](Sort(_))
    parameter("sort".as[Sort].*).map {
      case s if s.isEmpty => SortList.empty
      case s              => SortList(s.toList.reverse)
    }
  }

  private def locate(using FromStringUnmarshaller[IriBase]): Directive1[Option[Iri]] =
    parameter("locate".as[IriBase].?).map(_.map(_.value))

  private def id(using FromStringUnmarshaller[IriBase]): Directive1[Option[Iri]] =
    parameter("id".as[IriBase].?).map(_.map(_.value))

  private def deprecated = parameter("deprecated".as[Boolean].?)

  private def versionParams = (revParamOpt & tagParam).tmap(VersionParams(_, _))

  private def logParams(using BaseUri) =
    (parameter("createdBy".as[Subject].?) & createdAt & parameter("updatedBy".as[Subject].?) & updatedAt)
      .tmap(LogParam(_, _, _, _))

  private def types(using FromStringUnmarshaller[Type]): Directive1[List[Type]] =
    parameter("type".as[Type].*).map(_.toList.reverse)

  private def typeOperator(using FromStringUnmarshaller[TypeOperator]): Directive1[TypeOperator] =
    parameter("typeOperator".as[TypeOperator].?[TypeOperator](Or))

  private def typeParams(pc: Option[ProjectContext]) = {
    given FromStringUnmarshaller[Type] = Type.typeFromStringUnmarshaller(pc)
    (types & typeOperator).tmap(TypeParams(_, _))
  }

  private val keywords: Directive1[KeywordsParam] = {
    given FromStringUnmarshaller[Map[Label, String]] = Unmarshaller.strict { string =>
      parse(string).flatMap(_.as[Map[Label, String]]) match {
        case Left(e)      => throw e
        case Right(value) => value
      }
    }
    parameter(
      "keywords"
        .as[Map[Label, String]]
        .withDefault(Map.empty[Label, String])
    ).map(KeywordsParam(_))
  }

  private def schema(using FromStringUnmarshaller[IriBase]): Directive1[Option[ResourceRef]] =
    parameter("schema".as[IriBase].?).map(_.map(iri => ResourceRef(iri.value)))

  private def textQuery = parameter("q".?).map(_.filter(_.trim.nonEmpty).map(_.toLowerCase))

  private[routes] def searchParameters(pc: Option[ProjectContext])(using BaseUri): Directive1[ResourcesSearchParams] = {
    given FromStringUnmarshaller[IriBase] =
      pc.fold(iriBaseFromStringUnmarshallerNoExpansion)(iriBaseFromStringUnmarshaller(_))

    (locate & id & deprecated & versionParams & logParams & typeParams(pc) & keywords & schema & textQuery)
      .tmap(ResourcesSearchParams.apply)
  }

  private[routes] def searchParametersAndSortList(
      pc: Option[ProjectContext]
  )(using BaseUri): Directive[(ResourcesSearchParams, SortList)] =
    (searchParameters(pc) & sortList).tflatMap { case (params, sortList) =>
      if params.q.isDefined && !sortList.isEmpty then reject(simultaneousSortAndQRejection)
      else if params.q.isEmpty && sortList.isEmpty then tprovide((params, SortList.byCreationDateAndId))
      else tprovide((params, sortList))
    }

  /**
    * Extract the elasticsearch request from the entity and the query params
    * @return
    */
  def elasticSearchRequest: Directive1[ElasticSearchRequest] =
    (jsonObjectEntity & extractUri).tmap { case (body, uri) =>
      val params = uri.query().toMap -- searchParamsSortAndPaginationKeys
      ElasticSearchRequest(body, params)
    }

}

object ElasticSearchViewsDirectives extends ElasticSearchViewsDirectives
