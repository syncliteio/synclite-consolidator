package com.synclite.consolidator.web;


import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateDestinationDB
 */

/*
//Define all Data Types
HashMap<String, String[]> dataTypes = new HashMap<String, String[]>();
dataTypes.put("SQLITE", new HashSet<String>("integer", "tinyint", "smallint", "mediumint", "bigint", "unsigned bigint", "int2", "int8", "text", "clob", "blob", "real", "double", "double precision", "float", "numeric","boolean", "date", "datetime", "decimal"));
dataTypes.put("DUCKDB", new String[]{"integer", "tinyint", "smallint", "mediumint", "bigint", "unsigned bigint", "int2", "int8", "text", "clob", "blob", "real", "double", "double precision", "float", "numeric","boolean", "date", "datetime", "decimal"});
dataTypes.put("POSTGRESQL", new String[]{"integer", "smallserial", "serial", "bigserial", "smallint", "bigint", "long", "real", "double precision", "float", "bytea", "boolean", "clob", "blob", "real", "double", "double precision", "float", "numeric","boolean", "date", "datetime", "decimal"});
*/

@WebServlet("/validateDataTypeMappings")
public class ValidateDataTypeMappings extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateDataTypeMappings() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		Integer dstIndex = 1;
		try {
			
			if (request.getSession().getAttribute("num-destinations") == null) {
				throw new ServletException("\"Number Of Destination Databases\" not configured.");
			}			
			Integer numDestinations = Integer.valueOf(request.getSession().getAttribute("num-destinations").toString());
			String dstIndexStr = request.getParameter("dst-index");
			try {
				dstIndex = Integer.valueOf(dstIndexStr);
				if (dstIndex == null) {
					throw new ServletException("Please specify a valid numeric value for \"Destination Index\"");
				} else if (dstIndex <= 0 ) {
					throw new ServletException("Please specify a positive numeric value for \"Destination Index\"");
				} else if (dstIndex > numDestinations) {
					throw new ServletException("\"Destination Index\"" + dstIndex + " exceeded configured \"Number Of Destinations Databases\"");
				}
			} catch(NumberFormatException e) {
				throw new ServletException("Please specify a valid numeric value for \"Destination Index\"");
			}

			String dstDataTypeMapping = request.getParameter("dst-data-type-mapping-" + dstIndex);
		    String dstDataTypeAllMappings = request.getParameter("dst-data-type-all-mappings-" + dstIndex);		    
	    
			request.getSession().setAttribute("dst-data-type-mapping-" + dstIndex, dstDataTypeMapping);
			request.getSession().setAttribute("dst-data-type-all-mappings-" + dstIndex, dstDataTypeAllMappings);
			if (dstDataTypeMapping.equals("CUSTOMIZED")) {
				HashMap<String, String> mappingProps = prepareMappingProps(dstDataTypeAllMappings, dstIndex);
				request.getSession().setAttribute("all-mapping-props-" + dstIndex, mappingProps);
			} else {
				request.getSession().removeAttribute("all-mapping-props-" + dstIndex);
			}
			
			//request.getRequestDispatcher("configureFilterMapper.jsp?dstIndex=" + dstIndex).forward(request, response);
			response.sendRedirect("configureFilterMapper.jsp?dstIndex=" + dstIndex);

		} catch (Exception e) {
			//throw e;
			request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureDataTypes.jsp?dstIndex=" + dstIndex + "&errorMsg=" + errorMsg).forward(request, response);
		}
	}

	private final HashMap<String, String> prepareMappingProps(String dstDataTypeAllMappings, Integer dstIndex) throws ServletException {
		HashMap<String, String> mappingProps = new HashMap<String, String>();
		String[] propLines = dstDataTypeAllMappings.split("\n");
		
		if (propLines.length == 0) {
			throw new ServletException("No customized data type mappings specified");
		}
		for (int i=0 ; i < propLines.length ; ++i) {
			String[] tokens  = propLines[i].split("->");
			if (tokens.length != 2) {
				throw new ServletException("Invalid data type mapping specified : " + propLines[i]);
			}
			mappingProps.put("map-src-" + tokens[0].trim().toLowerCase() + "-to-dst-" + dstIndex , tokens[1].trim());			
		}
		return mappingProps;
	}
}
