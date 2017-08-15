package crossmatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.math3.analysis.function.Abs;
import org.apache.spark.broadcast.Broadcast;
import org.omg.PortableServer.ThreadPolicyOperations;

import healpix.essentials.HealpixProc;
import healpix.essentials.Moc;
import healpix.essentials.Pointing;
import healpix.essentials.RangeSet;
import healpix.essentials.Vec3;
import scala.Serializable;
import scala.Tuple2;

public class Elem implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public Elem()
    {
        id = 0;
    }
	
	static final double ToRadians360 = 2 * Math.PI;
	static final double ToRadians90 = 0.5 * Math.PI;
	
	public Elem(long id, Pointing p) {
		this.id = id;
		this.ra = p.phi;
		this.dec = p.theta;
	}

    public String ToString()
    {
        return new String(Long.toString(id) + "," + ra + "," + dec);
    }
    
    public static Pointing degrees2Point(double raDeg, double decDeg) {
    	Pointing pointing = new Pointing(Math.toRadians(90 - decDeg), Math.toRadians(360 - raDeg));
		return pointing;
    }
    
    public static long pixToLowerLevel(long pix, int currentLevel, int lowerLevel) throws Exception {
    	return HealpixProc.ang2pixRing(lowerLevel, HealpixProc.pix2angRing(currentLevel, pix));
    }
    
    public double dist(Elem e) {
    	/*Vec3 v1 = new Vec3(p());
    	Vec3 v2 = new Vec3(e.p());
    	v1 = v1.sub(v2);
    	return Math.sqrt(v1.x * v1.x + v1.y * v1.y + v1.z * v1.z);*/
    	double lat1 = Math.PI / 2 - dec;
    	double lat2 = Math.PI / 2 - e.dec;
    	double dLat = lat1 - lat2;
        double dLon = e.ra - ra;
    	
    	double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
        return 2 * Math.asin(Math.sqrt(a));
    }
    
    
    static public double dist(Pointing a, Pointing b) {
    	double lat1 = Math.PI / 2 - a.theta;
    	double lat2 = Math.PI / 2 - b.theta;
    	double dLat = lat1 - lat2;
        double dLon = b.phi - a.phi;
    	
    	double d = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
        return 2 * Math.asin(Math.sqrt(d));
    }
    
    public static long [] mocToArray(Moc moc, int neededLevel) {
    	moc = moc.degradedToOrder(neededLevel, true);
    	RangeSet rs = moc.getRangeSet();
    	
    	int shift = 2 * (29 - neededLevel);
    	System.out.println((int)(rs.nval() >>> shift));
    	long[] res = new long[(int)(rs.nval() >>> shift)];
        int ofs=0;
        for (int i = 0; i < rs.nranges(); ++i)
          for (long j = rs.ivbegin(i) >>> shift; j < rs.ivend(i) >>> shift; ++j)
            res[ofs++]=j;
        return res;
    }
    
    public double dummyFastDist(Elem e) {
		return dec - e.dec;
	}
    
    public Pointing p() { return new Pointing(dec, ra); }
    public long id;
    public double ra;
    public double dec;
    
    
}
