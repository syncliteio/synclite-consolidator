package com.synclite.consolidator.web;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Servlet implementation class ValidateDestinationDB
 */


@WebServlet("/validateDBTriggers")
public class ValidateDBTriggers extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor. 
	 */
	public ValidateDBTriggers() {
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

			String dstEnableTriggersStr = request.getParameter("dst-enable-triggers-" + dstIndex);

			if (dstEnableTriggersStr.equals("true")) {
				//Validate and persist
				
				String triggersStr = request.getParameter("dst-triggers-" + dstIndex);
				if (triggersStr.isBlank()) {
					throw new ServletException("You must specify trigger statements with triggers functionality enabled");					
				}
				
				try {
		            new JSONObject(new JSONTokener(triggersStr));
				} catch (Exception e) {
					throw new ServletException("Please specify trigger statements in valid JSON format", e);
				}
				String deviceDataRoot = request.getSession().getAttribute("device-data-root").toString();
				String triggersFilePath = Path.of(deviceDataRoot, "synclite_dst_triggers_" + dstIndexStr + ".json").toString();
				try {
					Files.writeString(Path.of(triggersFilePath), triggersStr);
				} catch (IOException e) {
					throw new ServletException("Failed to write triggers in configuration file : " + triggersFilePath, e);
				}
				request.getSession().setAttribute("dst-triggers-file-" + dstIndex, triggersFilePath);
			} else {
				request.getSession().removeAttribute("dst-triggers-file-" + dstIndex);
			}
			request.getSession().setAttribute("dst-enable-triggers-" + dstIndex, dstEnableTriggersStr);

			//request.getRequestDispatcher("configureDBWriter.jsp?dstIndex=" + dstIndex).forward(request, response);
			response.sendRedirect("configureDBWriter.jsp?dstIndex=" + dstIndex);
		} catch (Exception e) {
			//		request.setAttribute("saveStatus", "FAIL");
			System.out.println("exception : " + e);
			String errorMsg = e.getMessage();
			request.getRequestDispatcher("configureDBTriggers.jsp?dstIndex=" + dstIndex + "&errorMsg=" + errorMsg).forward(request, response);
		}
	}
}
