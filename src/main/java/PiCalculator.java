import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

public class PiCalculator
{
    private JavaSparkContext sc = new JavaSparkContext();
    private SQLContext sqlContext = new SQLContext(sc);

    public void calculatePi(int NUM_SAMPLES)
    {
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }

    public void doDataBaseStuff()
    {
        sqlContext.sql("CREATE DATABASE lassetest");
        sqlContext.sql("CREATE TABLE Persons (\n" +
                "    PersonID int,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");");
        sqlContext.sql("INSERT INTO Persons\n" +
                "VALUES (5, Norby, Pony, Ponystræde 69, Odense);");
        DataFrame dataFrame = sqlContext.sql("SELECT * FROM Persons WHERE PersonID = 5");
        dataFrame.show();

    }


    public static void main(String[] args)
    {
        PiCalculator pi = new PiCalculator();
        pi.doDataBaseStuff();
        //pi.calculatePi(10);
        //pi.calculatePi(Integer.parseInt(args[0]));
    }
}
