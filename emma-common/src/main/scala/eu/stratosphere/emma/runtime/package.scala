package eu.stratosphere.emma

import java.util.UUID
import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.slf4j.Logger
import eu.stratosphere.emma.api.{ParallelizedDataBag, DataBag}
import eu.stratosphere.emma.ir._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.runtime.universe._

package object runtime {

  // add new root file appender
  {
    val appender = new org.apache.log4j.RollingFileAppender()
    appender.setLayout(new org.apache.log4j.PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n"))
    appender.setFile(String.format("%s/emma.log", System.getProperty("emma.path.log", s"${System.getProperty("java.io.tmpdir")}/emma/log")), true, true, 4096)
    appender.setMaxFileSize("100KB")
    appender.setMaxBackupIndex(1)
    org.apache.log4j.Logger.getRootLogger.addAppender(appender)
  }

  private[emma] val logger = Logger(LoggerFactory.getLogger(classOf[Engine]))

  val mirror = runtimeMirror(getClass.getClassLoader)

  abstract class Engine {

    val envSessionID = UUID.randomUUID()
    var executionPlanJson: mutable.Stack[Plan] = new mutable.Stack[Plan]()

    var blockingLatch:CountDownLatch = null

    def setBlockingLatch(blocker: CountDownLatch) {
      blockingLatch = blocker
    }

    def getBlockingLatch():CountDownLatch = blockingLatch

    def getExecutionPlanJson: mutable.Stack[Plan] = executionPlanJson

    private var closed = false

    // log program run header
    {
      logger.info("############################################################")
      logger.info("# Emma: Parallel Dataflow Compiler")
      logger.info("############################################################")
      logger.info(s"Starting Emma session $envSessionID (${this.getClass.getSimpleName} runtime)")
    }

    val defaultDOP: Int

    def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A

    def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A]

    def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit

    def scatter[A: TypeTag](values: Seq[A]): DataBag[A]

    def gather[A: TypeTag](ref: DataBag[A]): DataBag[A]

    final def closeSession() = if (!closed) {
      doCloseSession()
      closed = true
    }

    protected def doCloseSession() = logger.info(s"Closing Emma session $envSessionID (${this.getClass.getSimpleName} runtime)")

    def getExecutionPlan(root:Combinator[_]) : String = root match {
      case Read(location: String, _) => buildJson(root, "Read", "")
      case Write(location: String, _, xs: Combinator[_]) => buildJson(root, "Write", getExecutionPlan(xs))
      case TempSource(ref) => buildJson(root, "TempSource", "")
      case TempSink(name: String, xs: Combinator[_]) => buildJson(root, "TempSink ("+name+")", getExecutionPlan(xs))
      case Map(_, xs: Combinator[_]) => buildJson(root, "Map", getExecutionPlan(xs))
      case FlatMap(_, xs: Combinator[_]) => buildJson(root, "FlatMap", getExecutionPlan(xs))
      case Filter(_, xs: Combinator[_]) => buildJson(root, "Filter", getExecutionPlan(xs))
      case EquiJoin(_, _, _, xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "EquiJoin", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
      case Cross(_, xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Cross", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
      case Group(_, xs: Combinator[_]) => buildJson(root, "Group", getExecutionPlan(xs))
      case Fold(_, _, _, xs: Combinator[_]) => buildJson(root, "Fold", getExecutionPlan(xs))
      case FoldGroup(_, _, _, _, xs: Combinator[_]) => buildJson(root, "FoldGroup", getExecutionPlan(xs))
      case Distinct(xs: Combinator[_]) => buildJson(root, "Distinct", getExecutionPlan(xs))
      case Union(xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Union", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
      case Diff(xs: Combinator[_], ys: Combinator[_]) => buildJson(root, "Diff", getExecutionPlan(xs)+", "+getExecutionPlan(ys))
      case Scatter(xs: Combinator[_]) => buildJson(root, "Scatter", getExecutionPlan(xs))
      case _ => s"[${root.getClass.getName}}]"
    }

    def buildJson(root:Combinator[_], name: String, parents: String): String = {
      var nodeType = ""

      var label = name

      name match {
        case "Read" => nodeType = "type:\"INPUT\", location:\""+root.asInstanceOf[Read[_]].location+"\", "
        case "Write" => nodeType = "type:\"INPUT\", location:\""+root.asInstanceOf[Write[_]].location+"\", "
        case "TempSource" => {
          val t = root.asInstanceOf[TempSource[_]]
          if (t.ref != null)
            label = "TempSource ("+t.ref.asInstanceOf[ParallelizedDataBag[_, _]].name+")"
        }
        case _ =>
      }
      s"""{\"id\":\"${System.identityHashCode(root)}\", \"label\":\"$label\", $nodeType\"parents\":[$parents]}"""
    }

    def addPlan(name:String, root:Combinator[_]) = {
      if (blockingLatch != null) {
        executionPlanJson.push(new Plan(name, getExecutionPlan(root)))
        blockingLatch.await()
      }
    }
  }

  case class Native() extends Engine {

    logger.info(s"Initializing native execution environment")

    sys addShutdownHook {
      closeSession()
    }

    override lazy val defaultDOP = 1

    override def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A = ???

    override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A] = ???

    override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = ???

    override def scatter[A: TypeTag](values: Seq[A]): DataBag[A] = ???

    override def gather[A: TypeTag](ref: DataBag[A]): DataBag[A] = ???
  }

  def default(): Engine = {
    val backend = System.getProperty("emma.execution.backend", "")
    val execmod = System.getProperty("emma.execution.mode", "")

    require(backend.isEmpty || ("native" :: "flink" :: "spark" :: Nil contains backend),
      "Unsupported execution backend (native|flink|spark).")
    require((backend.isEmpty || backend == "native") || ("local" :: "remote" :: Nil contains execmod),
      "Unsupported execution mode (local|remote).")

    backend match {
      case "flink" => execmod match {
        case "local" /* */ => factory("flink-local", "localhost", 6123)
        case "remote" /**/ => factory("flink-remote", "localhost", 6123)
      }
      case "spark" => execmod match {
        case "local" /* */ => factory("spark-local", s"local[${Runtime.getRuntime.availableProcessors()}]", 7077)
        case "remote" /**/ => factory("spark-remote", "localhost", 7077)
      }
      case _ => Native() // run native version of the code
    }
  }

  def factory(name: String, host: String, port: Int) = {
    // reflect engine
    val engineClazz = mirror.staticClass(s"${getClass.getPackage.getName}.${toCamelCase(name)}")
    val engineClazzMirror = mirror.reflectClass(engineClazz)
    val engineClassType = appliedType(engineClazz)

    if (!(engineClassType <:< typeOf[Engine]))
      throw new RuntimeException(s"Cannot instantiate engine '${getClass.getPackage.getName}.${toCamelCase(name)}' (should implement Engine)")

    if (engineClazz.isAbstract)
      throw new RuntimeException(s"Cannot instantiate engine '${getClass.getPackage.getName}.${toCamelCase(name)}' (cannot be abstract)")

    // reflect engine constructor
    val constructorMirror = engineClazzMirror.reflectConstructor(engineClassType.decl(termNames.CONSTRUCTOR).asMethod)
    // instantiate engine
    constructorMirror(host, port).asInstanceOf[Engine]
  }

  def toCamelCase(name: String) = name.split('-').map(x => x.capitalize).mkString("")
}

case class Plan(name: String, plan: String)