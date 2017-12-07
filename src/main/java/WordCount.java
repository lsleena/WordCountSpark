import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by leena on 12/6/17.
 */
public class WordCount {
    public static void main(String[] args) {
        if(args.length != 2){
            System.out.println("Please sepcify input file path");
            System.exit(1);
        }
        String fileName = args[0];
        String master = args[1];
        System.out.println("Hello World");
        SparkConf conf = new SparkConf().setAppName("Leena WordCount").setMaster(master);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> lines = sparkContext.textFile(fileName);

        System.out.println("First line " + lines.first() );
        System.out.println("Number of lines " + lines.count());
    }
}
