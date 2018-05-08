package tw.howie.spark;

import com.google.common.collect.Lists;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

/**
 * @author howie
 * @since 2018/5/8
 */
public class CodeGenErrorTest {

    String master = "local[2]";
    private SparkSession spark = SparkSession.builder()
                                             .appName("CodeGenErrorTest")
                                             .master(master)
                                             .config("spark.dynamicAllocation.enabled", true)
                                             .config("spark.shuffle.service.enabled", true)
                                             .config("spark.driver.maxResultSize", "4g")
                                             .config("spark.executor.memory", "6g")
                                             .config("spark.executor.cores", "2")
                                             .config("spark.cores.max", "4")
                                             .config("spark.submit.deployMode", "client")
                                             .config("spark.network.timeout", "3600s")
                                             .config("spark.eventLog.enabled", true)
                                             .config("spark.io.compression.codec", "snappy")
                                             .config("spark.rdd.compress", "true")
                                             .config("spark.rpc.message.maxSize", 256)
                                             .config("spark.sql.codegen.wholeStage", true) //workaround -> false
                                             .config("spark.serializer ", "org.apache.spark.serializer.KryoSerializer")
                                             .getOrCreate();

    @Before
    public void before() {
        PropertyConfigurator.configure(CodeGenErrorTest.class.getClassLoader()
                                                             .getResource("log4j-test.properties"));
    }

    @After
    public void after() {
        spark.stop();
    }

    @Test
    public void testCodeGenErrorByJoin() {

        StructType impressionSchema = new StructType(new StructField[] {DataTypes.createStructField("impressionCount",
                                                                                                    DataTypes.StringType,
                                                                                                    true),
                                                                        DataTypes.createStructField("impressionRate",
                                                                                                    DataTypes.StringType,
                                                                                                    true),

                                                                        DataTypes.createStructField("rid",
                                                                                                    DataTypes.StringType,
                                                                                                    true),

                                                                        DataTypes.createStructField("siteId",
                                                                                                    DataTypes.StringType,
                                                                                                    true),

                                                                        });

        StructType clickSchema = new StructType(new StructField[] {DataTypes.createStructField("clickCount",
                                                                                               DataTypes.StringType,
                                                                                               true),
                                                                   DataTypes.createStructField("rid",
                                                                                               DataTypes.StringType,
                                                                                               true),

                                                                   DataTypes.createStructField("siteId",
                                                                                               DataTypes.StringType,
                                                                                               true),

                                                                   });

        Dataset<Row> impression = spark.read()
                                       .schema(impressionSchema)
                                       .json("test1.log");

        Dataset<Row> click = spark.read()
                                  .schema(clickSchema)
                                  .json("test2.log");

        impression.show(false);
        click.show(false);

        List<String> list = Lists.newArrayList("siteId", "rid");

        Seq<String> col = JavaConverters.asScalaIteratorConverter(list.iterator())
                                        .asScala()
                                        .toSeq();

        Dataset<Row> join1 = impression.join(click, col, "leftouter");

        join1.show(false);
        // This result is different with CodeGenerator's debug message
        //        join1.queryExecution()
        //             .debug()
        //             .codegen();

        join1.write()
             .mode(SaveMode.Overwrite)
             .csv("/tmp/result");
    }
}
