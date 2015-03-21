package com.hortonworks.tutorials.ui.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.hortonworks.tutorials.ui.vo.Coordinate;

@Component("visualdao")
public class VisualDao {
	@Autowired
	private NamedParameterJdbcTemplate template;

	@Autowired(required = true)
	@Qualifier("query_properties")
	private Properties queries;

	private static Logger logger = Logger.getLogger(VisualDao.class);

	private static class CoordinateMapper implements RowMapper<Coordinate> {

		@Override
		public Coordinate mapRow(ResultSet rs, int rowNum) throws SQLException {
			Coordinate c = new Coordinate();
			c.setDriver_id(this.getValue(rs, "driver_id"));
			c.setDriver_name(this.getValue(rs, "driver_name"));
			c.setLatitude(this.getValue(rs, "latitude"));
			c.setLongitude(this.getValue(rs, "longitude"));
			c.setLongitude(this.getValue(rs, "longitude"));
			c.setRoute_id(this.getValue(rs, "route_id"));
			c.setRoute_name(this.getValue(rs, "route_name"));
			c.setTimestamp(this.getValue(rs, "timestamp"));
			c.setTotal_violations(this.getValue(rs, "total_violations"));
			c.setTruck_id(this.getValue(rs, "truck_id"));
			c.setViolation(this.getValue(rs, "violation"));
			return c;
		}

		private String getValue(ResultSet rs, String var) {
			try {
				String data = rs.getString(var);
				return data;
			} catch (SQLException b) {
				return null;
			}
		}
	}

	public List<Coordinate> getCoordinates() {
		String sql = queries.getProperty("get.truck.coordinates");
		Map<String, String> paramMap = new HashMap<String, String>();
		return template.query(sql, paramMap, new CoordinateMapper());
	}

}
