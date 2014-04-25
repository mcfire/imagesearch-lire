package net.semanticmetadata.lire.utils;

public class GeoUtils {

    public static double getGeoDistance(double latDegree1, double lngDegree1, double latDegree2, double lngDegree2) {
    	//地球直径（千米）
    	double R = 6371;
    	//两点经度之差的弧度值
    	double dLat = toRadians(latDegree2 - latDegree1);
    	//两点维度之差的弧度值
    	double dLon = toRadians(lngDegree2 - lngDegree1);
    	//将两点转换为弧度
    	double lat1 = toRadians(latDegree1);
    	double lat2 = toRadians(latDegree2);

    	//计算Haversine函数
    	double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
    	        Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2);
    	//计算c
    	double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
    	//计算两点间距离（千米）
    	double d = R * c;
    	return d;
    }
    
    private static double toRadians(double degrees) {
    	return degrees * Math.PI / 180;
    };

    private static double toDegrees(double radians) {
    	return radians * 180 / Math.PI;
    };
    
    public static void main(String[] args) {
    	double d = getGeoDistance(40.7486, -73.9864, 43.7486, -71.9864);
    	System.out.println(d);
    	d = getGeoDistance(1.7486, 2.9864, 3.7486, 4.9864);
    	System.out.println(d);
    	d = getGeoDistance(43.7486, 71.9864, 40.7486, 73.9864);
    	System.out.println(d);
    	System.out.print("end");
    }
}
