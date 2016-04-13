package se.sics.anomaly.bs.poission;

import org.apache.flink.api.java.tuple.Tuple3;
import se.sics.anomaly.bs.Gamma;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by martin on 2015-11-05.
 */
public class PoissonMicroModelRolling implements Serializable{
    private static final double prior_c = 0.0;

    private double[][] rollingHistory;

    private int numSegment;
    private int shiftPos;
    private int shiftNeg;
    private int currPos;
    private int multiplicty;

    public Tuple3<Double,Double,Integer> getHistory(){
        double totalCount = 0;
        double totalSize = 0;
        boolean noVal = false;
        for (int i = currPos - shiftNeg; i <= currPos + shiftPos; i++){

            double valCount = rollingHistory[wrapIndex(i)][0];
            double valSize = rollingHistory[wrapIndex(i)][1];

            totalCount += valCount;
            totalSize += valSize;

            if( valCount == -1d || valSize == -1d){
                noVal = true;
                break;
            }
        }
        if(noVal){
            return new Tuple3<>(-1d,-1d,currPos);
        }
        return new Tuple3<>(totalCount,totalSize,currPos);
    }

    public PoissonMicroModelRolling(int numSegment, int shiftPos, int shiftNeg){
        this.numSegment = numSegment;
        this.rollingHistory = new double[numSegment][2];
        for (double[] row: rollingHistory){
            Arrays.fill(row,-1d);
        }
        this.shiftNeg = shiftNeg;
        this.shiftPos = shiftPos;
        this.currPos = 0;
        this.multiplicty = 1;
    }

    public PoissonMicroModelRolling(int numSegment, int shiftPos, int shiftNeg, int multiplicty){
        this.numSegment = numSegment;
        this.rollingHistory = new double[numSegment][2];
        for (double[] row: rollingHistory){
            Arrays.fill(row,-1d);
        }
        this.shiftNeg = shiftNeg;
        this.shiftPos = shiftPos;
        this.currPos = 0;
        this.multiplicty = multiplicty;
    }


    public double calculateAnomaly(double elementCount, double windowSize){
        double totalCount = 0;
        double totalSize = 0;
        boolean noVal = false;

        for (int i = currPos - shiftNeg; i <= currPos + shiftPos; i++){
            double valCount = rollingHistory[wrapIndex(i)][0];
            double valSize = rollingHistory[wrapIndex(i)][1];

            totalCount += valCount;
            totalSize += valSize;

            if( valCount == -1d || valSize == -1d){
                noVal = true;
                break;
            }
        }

        currPos = wrapIndex(currPos+1);

        double res = -1d;
        if (!noVal) res = calculateAnomaly(elementCount,windowSize,totalCount,totalSize);
        System.out.println(res);
        return res;
    }

    public void addWindow(double elementCount, double windowSize){
        int pos = wrapIndex(currPos-1);
        rollingHistory[pos][0]=elementCount;
        rollingHistory[pos][1]=windowSize;
    }

    public void resetModel(){
        this.rollingHistory = new double[numSegment][2];
        for (double[] row: rollingHistory){
            Arrays.fill(row,-1d);
        }
        this.currPos = 0;
    }


    @Override
    public String toString() {
        return "PoissonMicroModel";
    }

    private int wrapIndex(int i) {
        int res = i % numSegment;
        if (res < 0) { // java modulus can be negative
            res += numSegment;
        }
        return res;
    }

    static double calculateAnomaly(double value, double windowSize, double modelValue, double modelSize)
    {
        // For now everything is calculated as poisson.
        if ((value == 0.0 && windowSize == 0.0) || modelSize == 0.0)
            return 0.0;
        else {
            double result = principal_anomaly_poisson(value, windowSize, modelValue, modelSize);
            return (result > 0.0 ? result > 1.0 ? 0.0 : -Math.log(result) : 700.0);
        }
    }


    // The principal anomaly for a poisson distribution
    public static double principal_anomaly_poisson(double value, double windowLength, double modelCount, double modelSize) {
        double sum = 0.0, lp1, lttu, luut;
        int top = (int) Math.floor(windowLength / modelSize * (modelCount - prior_c));
        int zz = (int) Math.round(value);

        // NOTE: code ported from c++ the array is a trick to do call by reference
        int[] l2 = new int[1];
        // the next line can throw infinity if u is 0.
        luut = Math.log(modelSize / (modelSize + windowLength));
        lttu = Math.log(windowLength / (modelSize + windowLength));
        lp1 = luut * (modelCount + 1.0 - prior_c) - Gamma.lngamma(modelCount + 1.0 - prior_c);
        if (top < value) {
            sum += pa_sum(top, -1, -1, zz, lp1, lttu, luut, modelCount, l2);
            sum += pa_sum(top + 1, zz, 1, zz, lp1, lttu, luut, modelCount, l2);
            sum += pa_sum(zz - 1, l2[0], -1, zz, lp1, lttu, luut, modelCount, l2);
            sum += pa_sum(zz, -1, 1, zz, lp1, lttu, luut, modelCount, l2);
        } else {
            sum += pa_sum(zz - 1, -1, -1, zz, lp1, lttu, luut, modelCount, l2);
            sum += pa_sum(zz, top, 1, zz, lp1, lttu, luut, modelCount, l2);
            sum += pa_sum(top, l2[0], -1, zz, lp1, lttu, luut, modelCount, l2);
            sum += pa_sum(top + 1, -1, 1, zz, lp1, lttu, luut, modelCount, l2);
        }

        return sum;
    }

    private static double pa_sum(int y1, int y2, int delta, int z, double lp1, double lttu, double luut, double s, int[] last) {
        double sum = 0.0, lp2, p3, lam, tmp = 1.0, peak = 0.0;
        int y;
        for (y = y1; y != y2 && tmp > peak * 1e-12; y += delta) {
            lp2 = lttu * y + Gamma.lngamma(s + 1.0 - prior_c + y) - Gamma.lngamma(1.0 + y);
            if (y != z) {
                lam = Math.exp((Gamma.lngamma(y + 1.0) - Gamma.lngamma(z + 1.0)) / (y - z) - lttu);
                p3 = (y < z ? Gamma.incgammaq(s + 1.0 - prior_c + y, lam) : Gamma.incgammap(s + 1.0 - prior_c + y, lam));
            } else
                p3 = 1.0;
            sum += tmp = Math.exp(lp1 + lp2) * p3;
            if (tmp > peak) peak = tmp;
        }
        last[0] = y - delta;
        return sum;
    }
}
