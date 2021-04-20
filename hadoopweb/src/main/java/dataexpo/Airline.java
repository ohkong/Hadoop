package dataexpo;

import org.apache.hadoop.io.Text;

public class Airline {
	private int year;
	private int month;
	private int arriveDelayTime;
	private int departureDelayTime;
	private int distance;
	private int cancel;
	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;
	private String uniqueCarrier;
	public Airline (Text text) {
		try {
			String[] columns = text.toString().split(",");
			year = Integer.parseInt(columns[0]);
			month = Integer.parseInt(columns[1]);
			uniqueCarrier = columns[8];	//항공사 코드
			if(!columns[14].equals("NA")) {	//도착지연 대상 비행기
				arriveDelayTime = Integer.parseInt(columns[14]);
			} else {	//도착지연 대상이 아닌 비행기
				arriveDelayAvailable = false;
			}
			if(!columns[15].equals("NA")) {	//출발지연 대상 비행기
				departureDelayTime =  Integer.parseInt(columns[15]);
			} else {	//츌발지연 대상이 아닌 비행기
				departureDelayAvailable = false;
			}
			if(!columns[18].equals("NA")) {//운항을 한 비행기의 운항거리
				distance = Integer.parseInt(columns[18]);
			} else {//운항 대상이 아닌 비행기
				distanceAvailable = false;
			}
			if(!columns[21].equals("0")) {
				cancel = 1;
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	//getter
	public int getYear() {
		return year;
	}
	public int getMonth() {
		return month;
	}
	
	public int getArriveDelayTime() {
		return arriveDelayTime;
	}
	
	public int getDepartureDelayTime() {
		return departureDelayTime;
	}
	
	public int getDistance() {
		return distance;
	}
	
	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}
	
	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}
	
	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}
	
	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
	
	public int getCancel() {
		return cancel;
	}
	
}
