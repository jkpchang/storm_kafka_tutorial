package com.hortonworks.tutorials.ui.rest;

import com.hortonworks.tutorials.ui.dao.HBaseDao;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hortonworks.tutorials.ui.dao.VisualDao;
import com.hortonworks.tutorials.ui.vo.Coordinate;
import java.io.IOException;

@Component
@Path("/coordinates")
public class VisualController {
	//@Autowired
	//private VisualDao visualDao;

	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getCompany() throws IOException {
		//List<Coordinate> coordinates = visualDao.getCoordinates();
		
                return Response.status(Status.OK).entity((new HBaseDao()).getCoordinates(1000)).build();

	}
}
