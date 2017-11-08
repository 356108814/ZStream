package com.ztesoft.zstream

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

/**
  * 日志处理
  * @author Yuri
  * @create 2017-11-7 10:23
  */
object Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null
  @volatile private var initialized = false

  // Method to get the logger name for this object
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Log methods that take only a String
  def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    if (!Logging.initialized) {
      initializeLogging(isInterpreter)
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        val defaultLogProps = "log4j.properties"
        Option(getClass.getClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            System.err.println(s"Using default log4j profile: $defaultLogProps")
          case None =>
            System.err.println(s"Unable to load $defaultLogProps")
        }
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val rootLogger = LogManager.getRootLogger()
        val replLogger = LogManager.getLogger(logName)
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel()) {
          System.err.printf("Setting default log level to \"%s\".\n", replLevel)
          rootLogger.setLevel(replLevel)
        }
      }
    }
    Logging.initialized = true

    log
  }
}
