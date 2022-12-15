import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object DFFromCSV{
    def main(args: Array[String]): Unit = {
        val spark:SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("Datamart")
            .getOrCreate()
        
        val df = spark.read.options(Map("delimiter" -> "\t", "header" -> "true")).csv("val_5000.csv")

        // best cols for clustering
        val selectColumns = Seq("code", "product_name", "energy_100g", "proteins_100g", "fat_100g", "carbohydrates_100g",
                            "sugars_100g", "energy-kcal_100g", "saturated-fat_100g", "salt_100g", "sodium_100g",
                            "fiber_100g", "fruits-vegetables-nuts-estimate-from-ingredients_100g", "nutrition-score-fr_100g")

        val data = df.select(selectColumns.head, selectColumns.tail: _*)
        data.write.options(Map("delimiter" -> "\t", "header" -> "true")).csv("preprocessed_data")

    }

}
DFFromCSV.main(Array("Datamart"))

