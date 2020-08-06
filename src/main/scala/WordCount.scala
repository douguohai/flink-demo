import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

object WordCount {

  def main(args: Array[String]): Unit = {
    var env = ExecutionEnvironment.getExecutionEnvironment
    var input = "/Users/tianwen/Documents/deployments/flink-study/src/main/resources/readme.txt"
    val inputDS: DataSet[String] = env.readTextFile(input)
    val wordCountDS: DataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1))
      .groupBy(0).sum(1)
    wordCountDS.print()
  }

}
