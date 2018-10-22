package pipeline

import java.nio.file.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Transformer {

  def transform(spark: SparkSession, dataFolder: Path): DataFrame = {
    ???
  }

}
