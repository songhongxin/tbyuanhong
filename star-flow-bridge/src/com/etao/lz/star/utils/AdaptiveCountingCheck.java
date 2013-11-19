package com.etao.lz.star.utils;

import com.clearspring.analytics.hash.Lookup3Hash;
import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;


public class AdaptiveCountingCheck extends AdaptiveCounting{
	
	
	public AdaptiveCountingCheck(int k)
    {
        super(k);
    
    }


    public AdaptiveCountingCheck(byte[] M) {
		super(M);
		// TODO Auto-generated constructor stub
	}	
	
	
   /* 只检测，不修改
    * 存在则返回false, 不存在返回ture
    * 
    * 2013-03-21
    * add by yuanhong.shx
    * */
    public boolean checkoffer(Object o)
    {
        boolean modified = false;

        long x = Lookup3Hash.lookup3ycs64(o.toString());
        int j = (int) (x >>> (Long.SIZE - k));
        byte r = (byte)(Long.numberOfLeadingZeros( (x << k) | (1<<(k-1)) )+1);
        if(M[j] < r)
        {
      //      Rsum += r-M[j];
      //     if(M[j] == 0) b_e--;
      //     M[j] = r;
            modified = true;
        }

        return modified;
    }
   
    /*
    
      public  static  void main(String []  args){
		
    	  AdaptiveCountingCheck session_uvstat  = new AdaptiveCountingCheck(Config.UV_BITMAP_K);
		
		System.out.println(session_uvstat.offer("hehe"));
		System.out.println(session_uvstat.offer("hehe"));
		System.out.println(session_uvstat.offer("hehe"));
		System.out.println(session_uvstat.cardinality());
		System.out.println(session_uvstat.offer("hhe"));
		System.out.println(session_uvstat.offer("hehe"));
		System.out.println(session_uvstat.cardinality());
		
		
		System.out.println("------------------------------");
		  AdaptiveCountingCheck session  = new AdaptiveCountingCheck(Config.UV_BITMAP_K);
		System.out.println(session.offer("hehe"));	
		System.out.println(session.checkoffer("hehe"));
		System.out.println(session.checkoffer("hehe"));
		System.out.println(session.checkoffer("hehe"));
		System.out.println(session.cardinality());
		System.out.println(session.offer("hhe"));
		System.out.println(session.offer("hehe"));
		System.out.println(session.cardinality());
		
		
	}
    
    */
    
    
    
}
