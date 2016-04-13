package se.sics.anomaly.bs.gaussian;

import se.sics.anomaly.bs.Gamma;

import java.io.Serializable;

/**
 * Created by mneumann on 2015-11-19.
 */
public class GaussianMicroModel implements Serializable{
    private double size;
    private double sums;
    private double sumsx;
    private double sumsxx;

    private double maxHistorySize;

    GaussianMicroModel(double maxHistorySize){
        this.maxHistorySize = maxHistorySize;
        this.sums= 0.0;
        this.sumsx= 0.0;
        this.sumsxx = 0.0;
        this.size= 0.0;
    }


    public void addWindow(double mean, double windowSize){
        // n+=1.0; sums+=windowSize; sumsx+=windowSize*value; sumsxx+=windowSize*windowValue*windowValue
        // cc=0.5*n; mean=sumsx/sums; scale=(sumsxx-sumsx*sumsx/sums);

        if(sums+windowSize > maxHistorySize){
            double factor = ((maxHistorySize-windowSize)/sums);
            this.size = size-1;
            this.sums = sums*factor;
            this.sumsx = sumsx*factor;
            this.sumsxx = sumsxx*factor;
        }

        this.size += 1.0;
        this.sums += windowSize;
        this.sumsx += windowSize*mean;
        this.sumsxx += windowSize*mean*mean;

    }

    public void removeWindow(double value, double windowSize){
        // size-=1.0; sums-=windowSize; sumsx-=value*value; sumsxx-=windowSize*value*value;
        this.size -= 1.0;
        this.sums -= windowSize;
        this.sumsx -= value*value;
        this.sumsxx -= windowSize*value*value;
    }

    public boolean isHistoryFull(){
        return sums >= maxHistorySize;
    }

    @Override
    public String toString() {
        return  "GaussianMicroModel{" +
                "maxHistorySize=" + maxHistorySize +
                ", size=" + size +
                " , sums="+sums +
                " , sumsx"+sumsx +
                " , sumsxx"+sumsxx +
                '}';
    }

    public double calculateAnomaly(double avgValue, double windoSize)
    {
        double mean = sumsx/sums;
        double scale = sumsxx-sumsx*sumsx/sums;
        double cc = size * 0.5;

        //avgValue, cc, mean, (sums+windowSize)/(sums*windowSize)*scale
        double res = calculateAnomaly(avgValue, cc, mean  , (sums+windoSize)/(sums*windoSize)*scale);
        if(Double.isNaN(res))return 0;
        return res;
    }

    private final static double M_PI = 3.14159265358979323846;
    private final static int MAXITER = 10000;

    public static double calculateAnomaly(double x, double c, double mn, double sc)
    {
        double dist = (x-mn)*(x-mn);
        return (dist==0.0 ? 0.0 : sc <= 0.0 ? 700.0 :
                -logIntStudent(c, Math.sqrt(dist/sc))-Math.log(2.0));
    }

    private static double logIntStudent(double c, double z)
    {
        if (z<0.0) z=-z;
        if (c*z*z >= 10.0)
            return Gamma.lngamma(c) - Gamma.lngamma(c+0.5) -0.5*Math.log(4.0*M_PI) - Math.log(1.0+z*z)*(c-0.5) +
                    Math.log(hyperGeometricBruteForce(c-0.5, 0.5, c+0.5, 1.0/(1.0+z*z)));
        else
            return Math.log(0.5 - Math.exp(Gamma.lngamma(c) - Gamma.lngamma(c-0.5) -Math.log(1.0+z*z)*c) / Math.sqrt(M_PI) * z *
                    hyperGeometricBruteForce(c, 1.0, 1.5, z*z/(1.0+z*z)));
    }

    private static double hyperGeometricBruteForce(double a, double b, double c, double z)
    {
        int n;
        double res, tmp, k;

        k = tmp = res = 1.0;
        for (n=1; n<MAXITER; n++) {
            k *= a*b/c * z/n;
            res += k;
            if (tmp == res) break;
            tmp = res;
            a += 1.0;
            b += 1.0;
            c += 1.0;
        }
        return res;
    }
}