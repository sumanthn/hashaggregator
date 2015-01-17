package sn.analytics.type;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Summarize the aggregated data
 * Created by Sumanth on 02/11/14.
 */
public class SummaryData {

    private final double min;
    private final double avg;
    private final double max;
    private  double sum;
    private  double median;
    private  double percentile90;
    private  double percentile95;
    public static final DecimalFormat formatter = new DecimalFormat("##.####");
    public SummaryData(double min, double avg, double max,double sum) {
        this.min = min;
        this.avg = avg;
        this.max = max;
        this.sum = sum;
    }

    public double getMin() {
        return min;
    }

    public double getAvg() {
        return avg;
    }

    public double getMax() {
        return max;
    }

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getPercentile90() {
        return percentile90;
    }

    public void setPercentile90(double percentile90) {
        this.percentile90 = percentile90;
    }

    public double getPercentile95() {
        return percentile95;
    }

    public void setPercentile95(double percentile95) {
        this.percentile95 = percentile95;
    }

    public double getSum() {
        return sum;
    }

    public static SummaryData EMPTY_INSTANCE(){return new SummaryData(0,0,0,0);}
    public String toString(){
        return "Min:" + min + " Max:" +max + " avg:"+ avg + " Median:" + median + " 90th:" +percentile90 + " 95th:" + percentile95;
    }

    public String toCsv(){
        StringBuilder sb = new StringBuilder();
        sb.append(formatter.format(min)).
                append(",").append(formatter.format(avg))
               // .append(",").append(formatter.format(median))
                .append(",").append(formatter.format(max));

               // append(",").append(formatter.format(percentile90))
               // .append(",").append(formatter.format(percentile95));
        return sb.toString();
    }
    
    
    
    public static void main(String [] args) throws IOException {
        
        final String delim = String.valueOf((char)(0x05));

        BufferedReader reader  = new BufferedReader(new FileReader("/Users/Sumanth/datadump/grindr/grindr"));

        String header = reader.readLine();
        String line = reader.readLine();
        reader.close();
        String [] tkns = header.split(delim);
        int count =0;
        for(String tkn : tkns){
            //if (tkn.contains("user"))
                System.out.println(tkn + "=" + count);

            count++;
        }
        
        String [] dataTkns = line.split(delim);
        System.out.println(dataTkns[24]);
        
        
    }
}
