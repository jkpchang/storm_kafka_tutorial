package com.hortonworks.tutorials.ui.vo;

public class Coordinate {
	private String driver_name;
	private String driver_id;
	private String route_name;
	private String route_id;
	private String truck_id;
	private String timestamp;
	private String longitude;
	private String latitude;
	private String violation;
	private String total_violations;
	
	public String getDriver_name() {
		return driver_name;
	}
	public void setDriver_name(String driver_name) {
		this.driver_name = driver_name;
	}
	public String getDriver_id() {
		return driver_id;
	}
	public void setDriver_id(String driver_id) {
		this.driver_id = driver_id;
	}
	public String getRoute_name() {
		return route_name;
	}
	public void setRoute_name(String route_name) {
		this.route_name = route_name;
	}
	public String getRoute_id() {
		return route_id;
	}
	public void setRoute_id(String route_id) {
		this.route_id = route_id;
	}
	public String getTruck_id() {
		return truck_id;
	}
	public void setTruck_id(String truck_id) {
		this.truck_id = truck_id;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getViolation() {
		return violation;
	}
	public void setViolation(String violation) {
		this.violation = violation;
	}
	public String getTotal_violations() {
		return total_violations;
	}
	public void setTotal_violations(String total_violations) {
		this.total_violations = total_violations;
	}
}
