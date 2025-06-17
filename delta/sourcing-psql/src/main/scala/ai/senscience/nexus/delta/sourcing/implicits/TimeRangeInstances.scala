package ai.senscience.nexus.delta.sourcing.implicits

import ai.senscience.nexus.delta.kernel.search.TimeRange
import ai.senscience.nexus.delta.kernel.search.TimeRange.*
import ai.senscience.nexus.delta.sourcing.FragmentEncoder
import doobie.postgres.implicits.*
import doobie.syntax.all.*
import doobie.util.fragment.Fragment

trait TimeRangeInstances {

  def createTimeRangeFragmentEncoder(columnName: String): FragmentEncoder[TimeRange] = {
    val column = Fragment.const(columnName)
    FragmentEncoder.instance {
      case Anytime             => None
      case After(value)        => Some(fr"$column >= $value")
      case Before(value)       => Some(fr"$column <= $value")
      case Between(start, end) => Some(fr"$column >= $start and $column <= $end")
    }
  }

}
