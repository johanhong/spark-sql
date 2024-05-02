import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PolicyDataProcessor {

    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("PolicyDataProcessor")
                .master("local") // Use local mode for testing
                .getOrCreate();

        // Create JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Generate sample policy data
        List<Row> policyRows = generatePolicyData();

        // Create RDD from list of rows
        JavaRDD<Row> policyRDD = sc.parallelize(policyRows);

        // Define schema for policy data
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("policyID", DataTypes.StringType, true),
                DataTypes.createStructField("policyType", DataTypes.StringType, true),
                DataTypes.createStructField("startDate", DataTypes.DateType, true),
                DataTypes.createStructField("endDate", DataTypes.DateType, true),
                DataTypes.createStructField("coverageAmount", DataTypes.DoubleType, true),
                DataTypes.createStructField("premiumAmount", DataTypes.DoubleType, true),
                DataTypes.createStructField("paymentFrequency", DataTypes.StringType, true),
                DataTypes.createStructField("policyStatus", DataTypes.StringType, true)
        ));

        // Create DataFrame from RDD using schema
        Dataset<Row> policyDF = spark.createDataFrame(policyRDD, schema);

        // Register DataFrame as a temporary view
        policyDF.createOrReplaceTempView("policy");

        // Perform SQL query to process policy data
        Dataset<Row> result = spark.sql("SELECT policyType, AVG(coverageAmount) AS avgCoverage " +
                "FROM policy " +
                "GROUP BY policyType ORDER BY avgCoverage");

        // Show result
        result.show();

        // Stop SparkSession
        spark.stop();
    }

    // Method to generate sample policy data
    private static List<Row> generatePolicyData() {
        List<Row> policyRows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String policyID = "POL" + i;
            String policyType = getRandomPolicyType();
            LocalDate startDate = getRandomDate(LocalDate.of(2010, 1, 1), LocalDate.of(2022, 1, 1));
            LocalDate endDate = startDate.plusYears((long) (Math.random() * 30));
            double coverageAmount = Math.round((10000 + (Math.random() * (1000000 - 10000))) * 100.0) / 100.0;
            double premiumAmount = Math.round((50 + (Math.random() * (5000 - 50))) * 100.0) / 100.0;
            String paymentFrequency = getRandomPaymentFrequency();
            String policyStatus = getRandomPolicyStatus();
            Row row = RowFactory.create(policyID, policyType, java.sql.Date.valueOf(startDate),
                    java.sql.Date.valueOf(endDate), coverageAmount, premiumAmount, paymentFrequency, policyStatus);
            policyRows.add(row);
        }
        return policyRows;
    }

    // Method to generate a random policy type
    private static String getRandomPolicyType() {
        String[] types = {"term", "whole life", "universal life"};
        int randomIndex = (int) (Math.random() * types.length);
        return types[randomIndex];
    }

    // Method to generate a random payment frequency
    private static String getRandomPaymentFrequency() {
        String[] frequencies = {"monthly", "quarterly", "semi-annually", "annually"};
        int randomIndex = (int) (Math.random() * frequencies.length);
        return frequencies[randomIndex];
    }

    // Method to generate a random policy status
    private static String getRandomPolicyStatus() {
        String[] statuses = {"active", "lapsed", "canceled"};
        int randomIndex = (int) (Math.random() * statuses.length);
        return statuses[randomIndex];
    }

    // Method to generate a random date within a range
    private static LocalDate getRandomDate(LocalDate startDate, LocalDate endDate) {
        long startEpochDay = startDate.toEpochDay();
        long endEpochDay = endDate.toEpochDay();
        long randomEpochDay = startEpochDay + (long) (Math.random() * (endEpochDay - startEpochDay));
        return LocalDate.ofEpochDay(randomEpochDay);
    }
}