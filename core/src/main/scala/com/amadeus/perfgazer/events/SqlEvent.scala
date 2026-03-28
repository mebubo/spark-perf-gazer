package com.amadeus.perfgazer.events

import org.apache.spark.sql.execution.SparkPlanInfo

/**
  * Raw event proving information about a SQL query
  */
case class SqlEvent(
  id: Long,
  description: String,
  physicalPlan: String,
  parsedLogicalPlan: Option[String] = None,
  analyzedLogicalPlan: Option[String] = None,
  optimizedLogicalPlan: Option[String] = None,
  planInfo: SparkPlanInfo
)

object SqlEvent {

  /** Enriches a SqlEvent with plans parsed from an extended explain string.
    * Only available in live mode; during event log replay qe is null and this is never called.
    */
  def withExtendedDetails(base: SqlEvent, extended: String): SqlEvent = {
    val sections = parseSections(extended)
    base.copy(
      physicalPlan          = sections.getOrElse("Physical Plan", base.physicalPlan),
      parsedLogicalPlan     = sections.get("Parsed Logical Plan"),
      analyzedLogicalPlan   = sections.get("Analyzed Logical Plan"),
      optimizedLogicalPlan  = sections.get("Optimized Logical Plan")
    )
  }

  private def parseSections(extended: String): Map[String, String] = {
    val header = "== (.+?) ==\n".r
    val matches = header.findAllMatchIn(extended).toList
    matches.zipWithIndex.map { case (m, i) =>
      val name  = m.group(1)
      val start = m.end
      val end   = if (i + 1 < matches.length) matches(i + 1).start else extended.length
      name -> extended.substring(start, end).trim
    }.toMap
  }
}
