package se.sics.anomaly.bs.models.normal;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import se.sics.anomaly.bs.core.AnomalyResult;
import se.sics.anomaly.bs.core.Gamma;
import se.sics.anomaly.bs.history.History;
import se.sics.anomaly.bs.models.Model;

import java.io.Serializable;

/**
 *
 * Created by mneumann on 2015-11-19.
 * ported from c++ library
 *
 * class IscGaussianAverageMicroModel : public IscMicroModel, public IscRawGaussianMicroModel {
 * public:
 * IscGaussianAverageMicroModel(int ix, int is) { indx=ix; inds=is; };
 * virtual ~IscGaussianAverageMicroModel() {};
 *
 * // Read out anomaly and log predicted prob
 * virtual double anomaly(intfloat* vec) { update(); return raw_anomaly(vec[indx].f, cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale); };
 * virtual double logp(intfloat* vec) { update(); return raw_logp(vec[indx].f, cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale); };
 *
 * virtual int ev_logp(intfloat* vec, double& e, double& v) { update(); raw_ev_logp(cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale, e, v); return 1; };
 * virtual int logpeak(intfloat* vec, double& p) { double dx; update(); p = raw_maxlogp(cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale, dx); return 1; };
 * virtual int invanomaly(intfloat* vec, double ano, intfloat* minvec, intfloat* maxvec, intfloat* peakvec) { double x1, x2; update(); if (minvec && maxvec) { raw_invanomaly(cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale, ano, x1, x2); minvec[indx].f=x1, maxvec[indx].f=x2; } if (peakvec) { raw_maxlogp(cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale, x1); peakvec[indx].f=x1; } return 1; };
 * virtual int stats(union intfloat* vec, double* expect, double* var) { double e, v; update(); raw_stats(cc, mean, (sums+vec[inds].f)/(sums*vec[inds].f)*scale, e, v); if (expect) expect[indx]=e; if (var) var[indx]=v; return 1; };
 *
 * // Training
 * virtual void add(intfloat* vec) { n+=1.0; sums+=vec[inds].f; sumsx+=vec[inds].f*vec[indx].f; sumsxx+=vec[inds].f*vec[indx].f*vec[indx].f; dirty = 1;};
 * virtual void remove(intfloat* vec) { n-=1.0; sums-=vec[inds].f; sumsx-=vec[inds].f*vec[indx].f; sumsxx-=vec[inds].f*vec[indx].f*vec[indx].f; dirty = 1; };
 * virtual void reset() { sums=0.0; sumsx=0.0; sumsxx = 0.0; n=0.0; dirty = 1; };
 * void update() { if (dirty) { cc=0.5*n; mean=sumsx/sums; scale=(sumsxx-sumsx*sumsx/sums); dirty=0; } };
 *
 * int dirty;
 * int indx, inds;
 * double sums, sumsx, sumsxx, n;
 * double cc, scale, mean;
 * };
 *
 */
public class NormalModel extends Model implements Serializable {
    private final static double M_PI = 3.14159265358979323846;
    private final static int MAXITER = 10000;

    private History hist;

    public NormalModel(History hist){
        this.hist = hist;
    }

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

    @Override
    public AnomalyResult calculateAnomaly(Tuple4<Double,Double,Long,Long> v, double threshold) {
        NormalHValue w =new NormalHValue();
        w.f0 = 1d;
        w.f1 = v.f1;
        w.f2 = v.f0 * v.f1;
        w.f3 = v.f0 * v.f0 * v.f1 ;

        NormalHValue h = (NormalHValue) hist.getHistory();
        if (h == null) {
            return new AnomalyResult(-1,v.f2,v.f3,threshold,w,h);
        }
        double mean = h.f2/h.f1;
        double scale = h.f3-h.f2*h.f2/h.f1;
        double cc = h.f0 * 0.5;
        return new AnomalyResult(calculateAnomaly(v.f0, cc, mean  , (h.f1+v.f1)/(h.f1*v.f1)*scale),v.f2,v.f3,threshold,w,h);
    }

    @Override
    public void addWindow(Tuple4<Double,Double,Long,Long> v) {
        NormalHValue hValue =new NormalHValue();
        hValue.f0 = 1d;
        hValue.f1 = v.f1;
        hValue.f2 = v.f0 * v.f1;
        hValue.f3 = v.f0 * v.f0 * v.f1 ;
        hist.addWindow(hValue);
    }

    @Override
    public TypeInformation getTypeInfo(){ return TypeInformation.of(new TypeHint<NormalModel>() {});}
}