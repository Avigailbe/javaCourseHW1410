package home_work1410;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;


public class MainLinkedIn {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("linkedIn").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");
        dataFrame.show();//file content
        dataFrame.printSchema();//schema
        Arrays.stream(dataFrame.dtypes()).forEach(System.out::println);//columns
        //dataFrame.groupBy(col("name"), explode(col("keywords")));
        // dataFrame.groupBy(col("name"), explode(col("keywords"))).count().sort("keywords").show();
        //System.out.println(dataFrame.withColumn("pop", explode(col("keywords"))).groupBy((col("name"))));
        //dataFrame.select(col("name"),explode(col("keywords"))).show();
        dataFrame.withColumn("salary", col("age").multiply(10).multiply(size(col("keywords")))).show();
        dataFrame.withColumn("salary", col("age").multiply(10).multiply(size(col("keywords"))))
                .where(col ("salary").leq(1200)).show();

    }

}
