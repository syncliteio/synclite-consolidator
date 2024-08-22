<%@page import="java.nio.file.Files"%>
<%@page import="java.nio.file.Path"%>
<%@ page import="javax.servlet.ServletContext" %>

<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<% 
	String version = "Version : Unknown";
	if (session.getAttribute("synclite-version") == null) {
		//Read from version file		
		try {
			// Get the real path of the WEB-INF directory
			String libPath = application.getRealPath("/WEB-INF/lib");
			Path versionFilePath = Path.of(libPath, "synclite.version");
			version = Files.readString(versionFilePath);
			session.setAttribute("synclite-version", version);
		} catch (Exception e) {
			//throw e;
		}
	} else {
		version = session.getAttribute("synclite-version").toString();
	}	
	out.print(version);
%>