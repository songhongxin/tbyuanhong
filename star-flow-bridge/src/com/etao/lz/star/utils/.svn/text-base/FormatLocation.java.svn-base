package com.etao.lz.star.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Vector;


/*
 * 
 * 获取location_id
 * 
 * @author  muqian 
 * 
 * */


public class FormatLocation implements java.io.Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2704982860782566178L;


	public static class MLocation {
		private long _start;
		private long _end;
		private int _cityId;
		private int _countryId;
		private int _provinceId;
		private long _locationId;
		
		public MLocation() {
			_start = _end = _locationId = _countryId = _provinceId = _cityId = 0;
		}
		
		public MLocation(long start, long end, long location, int country, int province, int city) {
			_start = start;
			_end = end;
			_locationId = location;
			_countryId = country;
			_provinceId = province;
			_cityId = city;
		}
		
		public String toString() {
			return String.format("{start:%d,end:%d,location:%d,country:%d,province:%d,city:%d}",
				_start,
				_end,
				_locationId,
				_countryId,
				_provinceId,
				_cityId
			);
		}
		
		public long getStart() { return _start;}
		public long getEnd() { return _end;}
		public long getLocationId() { return _locationId;}
		public int getCountryId() { return _countryId;}
		public int getProvinceId() { return _provinceId;}
		public int getCityId() { return _cityId;}
	}	
	
	private static Vector<MLocation> _ips = new Vector<MLocation>();
	
	public int getIpmapLen() {
		return _ips.size();
	}
	
	public void init() throws IOException {
		String ipResourcePath = "/ipmap.txt";
		String split = "\u0005";
		
		InputStream f = getClass().getResourceAsStream(ipResourcePath);
		if(f == null){
			
			System.out.println("load ipmap fail!!!");
		}
		initIps(f, split);
		
		if (f != null)
			f.close();
	}

	private void initIps(InputStream stream, String split) throws IOException {
		String line = null;
		BufferedReader bf = new BufferedReader(new InputStreamReader(stream));
		
		while((line = bf.readLine()) != null) {
			String[] lineArr = line.split(split, -1);
			int country = Integer.parseInt(lineArr[2]);
			int province = Integer.parseInt(lineArr[4]);
			int city = Integer.parseInt(lineArr[6]);
			
			_ips.add(new MLocation(
				Long.parseLong(lineArr[0]),
				Long.parseLong(lineArr[1]),
				country * 65536 + province * 256 + city,
				country,
				province,
				city
			));
		}
		
		/*
		for(int i = 0; i < _ips.size(); i++) {
			System.out.println(_ips.get(i).getStart());
		}
		*/
		
		if (bf != null)
			bf.close();
	}

	private MLocation scanIP(String ip) {
		String[] arr = ip.split("\\.");
		long l_ip = 0;
		
		if (arr.length == 4) {
			l_ip = Long.parseLong(arr[0]) * 16777216 + Long.parseLong(arr[1]) * 65536 + Long.parseLong(arr[2]) * 256 + Long.parseLong(arr[3]);
		}
		
		return search(l_ip, 0, _ips.size()-1);
	}
	
	/**
	 * 
	 * @param ipno
	 * @param pos 
	 * @return 0-ok 1-left 2-right
	 */
	private int compare(long ipno, int pos) {
		MLocation location = _ips.get(pos);
		
		if (ipno >= location.getStart() && ipno <= location.getEnd())
			return 0;
		else if (ipno < location.getStart())
			return 1;
		else
			return 2;
	}
	
	private MLocation search(long ipno, int left, int right) {
		int middle = left + (int)Math.ceil((right - left) / 2);
		if ((right - left) == 1) {
			if (compare(ipno, left) == 0) return _ips.get(left);
			if (compare(ipno, right) == 0) return _ips.get(right);
			return new MLocation();
		}
		if (right == left) {
			if (compare(ipno, left) == 0) return _ips.get(left);
			return new MLocation();
		}
		
		int result = compare(ipno, middle);
		if (result == 0)
			return _ips.get(middle);
		else if (result == 1)
			return search(ipno, left, middle - 1);
		else
			return search(ipno, middle + 1, right);
	}
	
	public long formatLocationid(String ip) {
		MLocation location = scanIP(ip);
		if (location != null) {
			return location.getLocationId();
		} else {
			return 0x00010101;
		}
	}

	/*
	public static void main(String[] args) {
		String ip = "125.46.5.134";
		FormatLocation form = new FormatLocation();
		try {
			form.init();
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		long loc = form.formatLocationid(ip);
		System.out.println(loc);
		
		Vector<MLocation> _ips2 = new Vector<MLocation>();
		_ips2.get(0);
	}
	*/
	
}
