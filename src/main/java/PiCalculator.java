import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PiCalculator
{
    private JavaSparkContext sc;

    public void calculatePi(int NUM_SAMPLES)
    {
        sc = new JavaSparkContext();
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


    public static void main(String[] args)
    {
        PiCalculator pi = new PiCalculator();
        pi.calculatePi(Integer.parseInt(args[0]));
    }
}
