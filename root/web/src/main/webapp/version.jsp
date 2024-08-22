<%-- 
    Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
    in compliance with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
    or implied.  See the License for the specific language governing permissions and limitations
    under the License.
--%>

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