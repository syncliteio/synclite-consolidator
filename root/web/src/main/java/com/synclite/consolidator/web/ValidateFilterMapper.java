package com.synclite.consolidator.web;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ValidateDestinationDB
 */


@WebServlet("/validateFilterMapper")
public class ValidateFilterMapper extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateFilterMapper() {
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**	  
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		Integer dstIndex = 1;
		try {

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

			String dstEnableFilterMapperRulesStr = request.getParameter("dst-enable-filter-mapper-rules-" + dstIndex);

			if (dstEnableFilterMapperRulesStr.equals("true")) {
				String dstAllowUnspecifiedTablesStr = request.getParameter("dst-allow-unspecified-tables-" + dstIndex);
				request.getSession().setAttribute("dst-allow-unspecified-tables-" + dstIndex, dstAllowUnspecifiedTablesStr);

				String dstAllowUnspecifiedColumnsStr = request.getParameter("dst-allow-unspecified-columns-" + dstIndex);
				request.getSession().setAttribute("dst-allow-unspecified-columns-" + dstIndex, dstAllowUnspecifiedColumnsStr);

				String dstFilterMapperRulesStr = request.getParameter("dst-filter-mapper-rules-" + dstIndex);

				String[] rules = dstFilterMapperRulesStr.split("\n");

				for (String rule : rules) {
					rule = rule.trim();
					if (rule.isBlank()) {
						continue;
					}
					String[] tokens = rule.split("=", 2);
					if (tokens.length == 2) {
						String key = tokens[0];
						String value = tokens[1];
						String[] keyTokens = key.split("\\.");
						if (keyTokens.length > 2) {
							throw new ServletException("Invalid column name specified : " + key);
						}
					} else {
						throw new ServletException("Invalid filter/mapper rule specified : " + rule);
					}
				}

				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
				String filterMapperConfigFilePath = Path.of(deviceDataRoot, "synclite_filter_mapper_rules_" + dstIndexStr + ".conf").toString();
				try {
					Files.writeString(Path.of(filterMapperConfigFilePath), dstFilterMapperRulesStr);
				} catch (IOException e) {
					throw new ServletException("Failed to write filter/mapper rules in configuration file : " + filterMapperConfigFilePath, e);
				}
				request.getSession().setAttribute("dst-filter-mapper-rules-file-" + dstIndex, filterMapperConfigFilePath);
			} else {
				request.getSession().removeAttribute("dst-allow-unspecified-tables-" + dstIndex);
				request.getSession().removeAttribute("dst-allow-unspecified-columns-" + dstIndex);
			}
			request.getSession().setAttribute("dst-enable-filter-mapper-rules-" + dstIndex, dstEnableFilterMapperRulesStr);

			//request.getRequestDispatcher("configureValueMapper.jsp?dstIndex=" + dstIndex).forward(request, response);
			response.sendRedirect("configureValueMapper.jsp?dstIndex=" + dstIndex);
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureFilterMapper.jsp?dstIndex=" + dstIndex + "&errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
