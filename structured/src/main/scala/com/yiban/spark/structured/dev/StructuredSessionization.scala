package com.yiban.spark.structured.dev

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

object StructuredSessionization {

  val logger = Logger.getLogger(SocketDemo.getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9999

    val spark = SparkSession
      .builder
      .appName("StructuredSessionization")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions",4)
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, treat words as sessionId of events
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

        // If timed out, then remove session and send final update
        if (state.hasTimedOut) {
          println("timeout...........")
          val finalUpdate =
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
        } else {
          println("no timeout...........")
          // Update start and end timestamps in session
          val timestamps = events.map(_.timestamp.getTime).toSeq
          val updatedSession = if (state.exists) {
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          state.update(updatedSession)

          // Set timeout such that the session will be expired if no data received for 10 seconds
          /**
            * 这里的timeout 还跟输入的数据有关，如果新的输入数据和上次一样，那么无论过了多久，都不会timeout
            * 但是一旦有不同的数据流进来，只要超过了设置的时间，以前的数据就会失效
            * 如果没超过设置的失效时间，那么以前的数据立马失效
            */
          state.setTimeoutDuration("10 seconds")
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
/** User-defined data type representing the input events */
case class Event(sessionId: String, timestamp: Timestamp)

/**
  * User-defined data type for storing a session information as state in mapGroupsWithState.
  *
  * @param numEvents        total number of events received in the session
  * @param startTimestampMs timestamp of first event received in the session when it started
  * @param endTimestampMs   timestamp of last event received in the session before it expired
  */
case class SessionInfo(
                        numEvents: Int,
                        startTimestampMs: Long,
                        endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
  * User-defined data type representing the update information returned by mapGroupsWithState.
  *
  * @param id          Id of the session
  * @param durationMs  Duration the session was active, that is, from first event to its expiry
  * @param numEvents   Number of events received by the session while it was active
  * @param expired     Is the session active or expired
  */
case class SessionUpdate(
                          id: String,
                          durationMs: Long,
                          numEvents: Int,
                          expired: Boolean)
