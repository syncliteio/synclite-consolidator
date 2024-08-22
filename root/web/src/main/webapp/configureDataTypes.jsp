<%@page import="com.synclite.consolidator.web.DstType"%>
<%@page import="java.nio.file.Files"%>
<%@page import="java.time.Instant"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.Map"%>
<%@page import="java.util.HashMap"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<%
HashMap<String, Object> properties = new HashMap<String, Object>();

if (session.getAttribute("device-data-root") != null) {
	properties.put("device-data-root", session.getAttribute("device-data-root").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

Integer dstIndex = 1;

if (request.getParameter("dstIndex") != null) {
	dstIndex = Integer.valueOf(request.getParameter("dstIndex"));
}
Integer numDestinations = Integer.valueOf(session.getAttribute("num-destinations").toString());

String errorMsg = request.getParameter("errorMsg");

properties.put("dst-type-" + dstIndex, session.getAttribute("dst-type-" + dstIndex).toString());
properties.put("dst-type-name-" + dstIndex, session.getAttribute("dst-type-name-" + dstIndex).toString());
properties.put("dst-alias-" + dstIndex, session.getAttribute("dst-alias-" + dstIndex).toString());
properties.put("src-app-type", session.getAttribute("src-app-type").toString());
if (request.getParameter("dst-data-type-mapping-" + dstIndex) != null) {
	//populate from request object
	properties.put("dst-data-type-mapping-" + dstIndex, request.getParameter("dst-data-type-mapping-" + dstIndex));
	properties.put("dst-data-type-all-mappings-" + dstIndex, request.getParameter("dst-data-type-all-mappings-" + dstIndex));
} else {
	//Populate default values in the map
	
	if (properties.get("src-app-type").toString().equals("SYNCLITE-DBREADER")) {
		properties.put("dst-data-type-mapping-" + dstIndex, "BEST_EFFORT");
	} else {
		if (properties.get("dst-type-" + dstIndex).toString().equals("SQLITE")) {
			properties.put("dst-data-type-mapping-" + dstIndex, "EXACT");	
		} else if (properties.get("dst-type-" + dstIndex).toString().equals("DUCKDB")) {
			properties.put("dst-data-type-mapping-" + dstIndex, "EXACT");
		} else {
			properties.put("dst-data-type-mapping-" + dstIndex, "ALL_TEXT");
		}
	}
	
	properties.put("dst-data-type-all-mappings-" + dstIndex, "");
	
	//Read configs from syncJob.props if they are present
	Path propsPath = Path.of(properties.get("device-data-root").toString(), "synclite_consolidator.conf");
	BufferedReader reader = null;
	try {
		if (Files.exists(propsPath)) {
	   	    reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (!line.startsWith("=")) {								
							properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
						}
					}
				}  else {
					properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
				}					
				line = reader.readLine();
			}
			reader.close();
		}
	} catch (Exception e){ 
		if (reader != null) {
			reader.close();
		}
		throw e;
	}
	
	//Check if there were any data type mappings loaded up from the props file
	

	if (properties.get("dst-data-type-mapping-" + dstIndex).equals("CUSTOMIZED")) {
		StringBuilder customizedPropsBuilder = new StringBuilder();
		customizedPropsBuilder.append("");
		for (Map.Entry<String, Object> entry : properties.entrySet()) {
			String pName = entry.getKey();
			
			if (pName.startsWith("map-src-") && pName.endsWith("-" + dstIndex)) {
				String[] tokens = pName.split("-");
				customizedPropsBuilder.append(tokens[2].toUpperCase() + " -> " + entry.getValue().toString() + "\n");				
			}
		}
		properties.put("dst-data-type-all-mappings-" + dstIndex, customizedPropsBuilder.toString());	
	}	
}

%>

<script type="text/javascript">
	 function populateDefaults() {
		var dstDataTypeMapping = document.getElementById("dst-data-type-mapping-<%=dstIndex%>").value;
		var dstType = document.getElementById("dst-type-<%=dstIndex%>").value;
		if (dstType.toString() === "SQLITE") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> TEXT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BLOB\n' +
				'REAL -> TEXT\n' +
				'BOOLEAN -> TEXT\n' +
				'DATE -> TEXT\n' +
				'DATETIME -> TEXT\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BLOB\n' +
				'REAL -> REAL\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> DATE\n' +
				'DATETIME -> DATETIME\n';
			}
		} else if (dstType.toString() === "DUCKDB") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> TEXT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BLOB\n' +
				'REAL -> TEXT\n' +
				'BOOLEAN -> TEXT\n' +
				'DATE -> TEXT\n' +
				'DATETIME -> TEXT\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BLOB\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "MONGODB") {
			document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = '';
	  	} else if (dstType.toString() === "COSMOSDB_MONGODB") {
			document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = '';
	  	} else if (dstType.toString() === "FERRETDB") {
			document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = '';
	  	} else if (dstType.toString() === "CSV") {
			document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = '';	  	
	  	} else if (dstType.toString() === "AZURE_EVENTHUB") {
			document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = '';		  	
	  	} else if (dstType.toString() === "MSSQL") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> NVARCHAR(MAX)\n' +
				'TEXT -> NVARCHAR(MAX)\n' +
				'CLOB -> NVARCHAR(MAX)\n' +
				'BLOB -> VARBINARY(MAX)\n' +
				'REAL -> NVARCHAR(MAX)\n' +
				'BOOLEAN -> NVARCHAR(MAX)\n' +
				'DATE -> NVARCHAR(MAX)\n' +
				'DATETIME -> NVARCHAR(MAX)\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> NVARCHAR(MAX)\n' +
				'CLOB -> NVARCHAR(MAX)\n' +
				'BLOB -> VARBINARY(MAX)\n' +
				'REAL -> FLOAT\n' +
				'BOOLEAN -> BIT\n' +
				'DATE -> DATETIME\n' +
				'DATETIME -> DATETIME\n';
			}
		} else if (dstType.toString() === "MSFABRICDW") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> NVARCHAR(MAX)\n' +
				'TEXT -> STRING(MAX)\n' +
				'CLOB -> STRING(MAX)\n' +
				'BLOB -> STRING(MAX)\n' +
				'REAL -> STRING(MAX)\n' +
				'BOOLEAN -> STRING(MAX)\n' +
				'DATE -> STRING(MAX)\n' +
				'DATETIME -> STRING(MAX)\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> STRING(MAX)\n' +
				'CLOB -> STRING(MAX)\n' +
				'BLOB -> STRING(MAX)\n' +
				'REAL -> FLOAT\n' +
				'BOOLEAN -> BIT\n' +
				'DATE -> DATETIME2(6)\n' +
				'DATETIME -> DATETIME2(6)\n';
			}
		} else if (dstType.toString() === "MYSQL") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> TEXT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> LONGTEXT\n' +
				'BLOB -> LONGBLOB\n' +
				'REAL -> TEXT\n' +
				'BOOLEAN -> TEXT\n' +
				'DATE -> TEXT\n' +
				'DATETIME -> TEXT\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> LONGTEXT\n' +
				'BLOB -> LONGBLOB\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		}  else if ((dstType.toString() === "POSTGRESQL") || (dstType.toString() === "PARADEDB")) {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> TEXT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BYTEA\n' +
				'REAL -> TEXT\n' +
				'BOOLEAN -> TEXT\n' +
				'DATE -> TEXT\n' +
				'DATETIME -> TEXT\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BYTEA\n' +
				'REAL -> DOUBLE PRECISION\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "SNOWFLAKE") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> TEXT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BINARY\n' +
				'REAL -> TEXT\n' +
				'BOOLEAN -> TEXT\n' +
				'DATE -> TEXT\n' +
				'DATETIME -> TEXT\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> TEXT\n' +
				'CLOB -> TEXT\n' +
				'BLOB -> BINARY\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "ORACLE") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> VARCHAR2(4000)\n' +
				'TEXT -> VARCHAR2(4000)\n' +
				'CLOB -> CLOB\n' +
				'BLOB -> BLOB\n' +
				'REAL -> VARCHAR2(4000)\n' +
				'BOOLEAN -> VARCHAR2(4000)\n' +
				'DATE -> VARCHAR2(4000)\n' +
				'DATETIME -> VARCHAR2(4000)\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> NUMBER\n' +
				'TEXT -> VARCHAR2(4000)\n' +
				'CLOB -> CLOB\n' +
				'BLOB -> BLOB\n' +
				'REAL -> NUMBER\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "DB2") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> VARCHAR(32672)\n' +
				'TEXT -> VARCHAR(32672)\n' +
				'CLOB -> CLOB\n' +
				'BLOB -> BLOB\n' +
				'REAL -> VARCHAR(32672)\n' +
				'BOOLEAN -> VARCHAR(32672)\n' +
				'DATE -> VARCHAR(32672)\n' +
				'DATETIME -> VARCHAR(32672)\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> VARCHAR(32672)\n' +
				'CLOB -> CLOB\n' +
				'BLOB -> BLOB\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "CLICKHOUSE") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> String\n' +
				'TEXT -> String\n' +
				'CLOB -> String\n' +
				'BLOB -> Blob\n' +
				'REAL -> String\n' +
				'BOOLEAN -> String\n' +
				'DATE -> String\n' +
				'DATETIME -> String\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> Int64\n' +
				'TEXT -> String\n' +
				'CLOB -> Clob\n' +
				'BLOB -> Blob\n' +
				'REAL -> Float64\n' +
				'BOOLEAN -> Bool\n' +
				'DATE -> DateTime\n' +
				'DATETIME -> DateTime\n';
			}
		} else if (dstType.toString() === "REDSHIFT") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> VARCHAR(MAX)\n' +
				'TEXT -> VARCHAR(MAX)\n' +
				'CLOB -> VARCHAR(MAX)\n' +
				'BLOB -> BYTEA\n' +
				'REAL -> VARCHAR(MAX)\n' +
				'BOOLEAN -> VARCHAR(MAX)\n' +
				'DATE -> VARCHAR(MAX)\n' +
				'DATETIME -> VARCHAR(MAX)\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> VARCHAR(MAX)\n' +
				'CLOB -> VARCHAR(MAX)\n' +
				'BLOB -> BYTEA\n' +
				'REAL -> DOUBLE PRECISION\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "DATABRICKS_SQL") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> STIRNG\n' +
				'TEXT -> STRING\n' +
				'CLOB -> STRING\n' +
				'BLOB -> BINARY\n' +
				'REAL -> STRING\n' +
				'BOOLEAN -> STRING\n' +
				'DATE -> STRING\n' +
				'DATETIME -> STRING\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> STRING\n' +
				'CLOB -> STRING\n' +
				'BLOB -> BINARY\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "APACHE_ICEBERG") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> STRING\n' +
				'TEXT -> STRING\n' +
				'CLOB -> STRING\n' +
				'BLOB -> BINARY\n' +
				'REAL -> STRING\n' +
				'BOOLEAN -> STRING\n' +
				'DATE -> STRING\n' +
				'DATETIME -> STRING\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> LONG\n' +
				'TEXT -> STRING\n' +
				'CLOB -> STRING\n' +
				'BLOB -> STRING\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> TIMESTAMP\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		} else if (dstType.toString() === "APACHE_HIVE") {
			if (dstDataTypeMapping === "ALL_TEXT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> STRING\n' +
				'TEXT -> STRING\n' +
				'CLOB -> STRING\n' +
				'BLOB -> STRING\n' +
				'REAL -> STRING\n' +
				'BOOLEAN -> STRING\n' +
				'DATE -> STRING\n' +
				'DATETIME -> STRING\n';
			} else if (dstDataTypeMapping === "BEST_EFFORT") {
				document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = 'INTEGER -> BIGINT\n' +
				'TEXT -> STRING\n' +
				'CLOB -> STRING\n' +
				'BLOB -> STRING\n' +
				'REAL -> DOUBLE\n' +
				'BOOLEAN -> BOOLEAN\n' +
				'DATE -> DATE\n' +
				'DATETIME -> TIMESTAMP\n';
			}
		}
		//document.getElementById("dst-data-type-all-mappings").value = document.getElementById("dst-data-type-all-mappings").innerHTML;
		//document.getElementById("dst-data-type-all-mappings").innerText = document.getElementById("dst-data-type-all-mappings").innerHTML;
		if (dstDataTypeMapping === "CUSTOMIZED") {
		    document.getElementById("dst-data-type-all-mappings-" + "<%=dstIndex%>").readOnly = false;			
		} else {
		    document.getElementById("dst-data-type-all-mappings-" + "<%=dstIndex%>").readOnly = true;
		}
		if (dstDataTypeMapping === "EXACT") {
			document.getElementById("dst-data-type-all-mappings-<%=dstIndex%>").innerHTML = '';
		}

	 }
</script>
<title>Configure Data type Mappings For Destination <%=dstIndex%>></title>
</head>
<body onload="populateDefaults()">
	<%@include file="html/menu.html"%>	

	<div class="main">
		<%		
		if (numDestinations == 1) {
			out.print("<h2>Configure Data Types : SyncLite -> " + properties.get("dst-type-name-" + dstIndex) + "</h2>");
		} else { 
			out.print("<h2>Configure Data Types : SyncLite -> " + properties.get("dst-type-name-" + dstIndex) + " (Destination DB " + dstIndex + " : " + properties.get("dst-alias-" + dstIndex) + ")</h2> ");
		}
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>
		<form action="${pageContext.request.contextPath}/validateDataTypeMappings" method="post">
			<input type="hidden" name ="dst-index" id ="dst-index" value="<%=dstIndex%>">
		
			<table>
				<tbody>
					<tr>
						<td>Destination Database Type</td>
						<td><select id="dst-type-<%=dstIndex%>" name="dst-type-<%=dstIndex%>" disabled>
								<%
								if (properties.get("dst-type-" + dstIndex).equals("AMAZON_REDSHIFT")) {									
									out.println("<option value=\"AMAZON_REDSHIFT\" selected>Amazon Redshift</option>");
								} else {
									out.println("<option value=\"AMAZON_REDSHIFT\">Amazon Redshift</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("APACHE_ICEBERG")) {									
									out.println("<option value=\"APACHE_ICEBERG\" selected>Apache Iceberg</option>");
								} else {
									out.println("<option value=\"APACHE_ICEBERG\">Apache Iceberg</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("COSMOSDB_MONGODB")) {
									out.println("<option value=\"COSMOSDB_MONGODB\" selected>Azure CosmosDB for MongoDB</option>");
								} else {
									out.println("<option value=\"COSMOSDB_MONGODB\">Azure CosmosDB for MongoDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("AZURE_EVENTHUB")) {									
									out.println("<option value=\"AZURE_EVENTHUB\" selected>Azure Event Hub</option>");
								} else {
									out.println("<option value=\"AZURE_EVENTHUB\">Azure Event Hub</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("CLICKHOUSE")) {
									out.println("<option value=\"CLICKHOUSE\" selected>ClickHouse</option>");
								} else {
									out.println("<option value=\"CLICKHOUSE\">ClickHouse</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("CSV")) {
									out.println("<option value=\"CSV\" selected>CSV Files</option>");
								} else {
									out.println("<option value=\"CSV\">CSV Files</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("DATABRICKS_SQL")) {									
									out.println("<option value=\"DATABRICKS_SQL\" selected>Databricks SQL</option>");
								} else {
									out.println("<option value=\"DATABRICKS_SQL\">Databricks SQL</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("DATALAKE")) {
									out.println("<option value=\"DATALAKE\" selected>Data Lake</option>");
								} else {
									out.println("<option value=\"DATALAKE\">Data Lake</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("DUCKDB")) {
									out.println("<option value=\"DUCKDB\" selected>DuckDB</option>");
								} else {
									out.println("<option value=\"DUCKDB\">DuckDB</option>");
								}								
								if (properties.get("dst-type-" + dstIndex).equals("FERRETDB")) {
									out.println("<option value=\"FERRETDB\" selected>FerretDB</option>");
								} else {
									out.println("<option value=\"FERRETDB\">FerretDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("DB2")) {
									out.println("<option value=\"DB2\" selected>IBM Db2</option>");
								} else {
									out.println("<option value=\"DB2\">IBM Db2</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("MSFABRICDW")) {
									out.println("<option value=\"MSFABRICDW\" selected>Microsoft Fabric DW</option>");
								} else {
									out.println("<option value=\"MSFABRICDW\">Microsoft Fabric DW</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("MSSQL")) {
									out.println("<option value=\"MSSQL\" selected>Microsoft SQL Server</option>");
								} else {
									out.println("<option value=\"MSSQL\">Microsoft SQL Server</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("MONGODB")) {
									out.println("<option value=\"MONGODB\" selected>MongoDB</option>");
								} else {
									out.println("<option value=\"MONGODB\">MongoDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("MYSQL")) {
									out.println("<option value=\"MYSQL\" selected>MySQL</option>");
								} else {
									out.println("<option value=\"MYSQL\">MySQL</option>");
								}							
								if (properties.get("dst-type-" + dstIndex).equals("ORACLE")) {
									out.println("<option value=\"ORACLE\" selected>Oracle</option>");
								} else {
									out.println("<option value=\"ORACLE\">Oracle</option>");
								} if (properties.get("dst-type-" + dstIndex).equals("PARADEDB")) {
									out.println("<option value=\"PARADEDB\" selected>ParadeDB</option>");
								} else {
									out.println("<option value=\"PARADEDB\">ParadeDB</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("POSTGRESQL")) {
									out.println("<option value=\"POSTGRESQL\" selected>PostgreSQL</option>");
								} else {
									out.println("<option value=\"POSTGRESQL\">PostgreSQL</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("SNOWFLAKE")) {
									out.println("<option value=\"SNOWFLAKE\" selected>Snowflake</option>");
								} else {
									out.println("<option value=\"SNOWFLAKE\">Snowflake</option>");
								}
								if (properties.get("dst-type-" + dstIndex).equals("SQLITE")) {
									out.println("<option value=\"SQLITE\" selected>SQLite</option>");
								} else {
									out.println("<option value=\"SQLITE\">SQLite</option>");
								}
								%>
						</select></td>
					</tr>
					<tr>
						<td>Data Type Mappings</td>
						<td><select id="dst-data-type-mapping-<%=dstIndex%>" name="dst-data-type-mapping-<%=dstIndex%>" onchange="populateDefaults()" title="Select an appropriate data type mapping depending on your data consolidation usecase. 'All Text' indicates that a column with any data type will be mapped to a variable length TEXT/VARCHAR (or an equivalent) data type on the destination type, this option is the most conservative.'Best Effort' indicates that the most appropriate matching data type will be identified for each source data type and used on the destination database. Choose 'Exact' when you have already created columns with final desired data types in the device tables which will work as is for your destination database.'Customize' allows you to override the system behavior and specify your own data type mappings in the below field.">
								<%

								if (properties.get("dst-type-" + dstIndex).equals("MONGODB")) {
									out.println("<option value=\"BEST_EFFORT\" selected>Best Effort Match</option>");
								} else if (properties.get("dst-type-" + dstIndex).equals("COSMOSDB_MONGODB")) {
									out.println("<option value=\"BEST_EFFORT\" selected>Best Effort Match</option>");
								} else if (properties.get("dst-type-" + dstIndex).equals("FERRETDB")) {
									out.println("<option value=\"BEST_EFFORT\" selected>Best Effort Match</option>");
								} else if (properties.get("dst-type-" + dstIndex).equals("CSV")) {
									out.println("<option value=\"ALL_TEXT\" selected>All Text</option>");
								} else if (properties.get("dst-type-" + dstIndex).equals("AZURE_EVENTHUB")) {
									out.println("<option value=\"ALL_TEXT\" selected>All Text</option>");
								} else {
									if (properties.get("dst-data-type-mapping-" + dstIndex).equals("ALL_TEXT")) {
										out.println("<option value=\"ALL_TEXT\" selected>All Text</option>");
									} else {
										out.println("<option value=\"ALL_TEXT\">All Text</option>");
									}
									if (properties.get("dst-data-type-mapping-" + dstIndex).equals("BEST_EFFORT")) {
										out.println("<option value=\"BEST_EFFORT\" selected>Best Effort Match</option>");
									} else {
										out.println("<option value=\"BEST_EFFORT\">Best Effort Match</option>");
									}
									if (properties.get("dst-data-type-mapping-" + dstIndex).equals("CUSTOMIZED")) {
										out.println("<option value=\"CUSTOMIZED\" selected>Customize</option>");
									} else {
										out.println("<option value=\"CUSTOMIZED\">Customize</option>");
									}
									if (properties.get("dst-data-type-mapping-" + dstIndex).equals("EXACT")) {
										out.println("<option value=\"EXACT\" selected>Exact</option>");
									} else {
										out.println("<option value=\"EXACT\">Exact</option>");
									}
								}
								%>
						</select></td>
					</tr>

					<tr>
						<td>Source Type -> Destination Type</td>						
						<td><textarea name="dst-data-type-all-mappings-<%=dstIndex%>" id="dst-data-type-all-mappings-<%=dstIndex%>" value="<%=properties.get("dst-data-type-all-mappings-" + dstIndex)%>" 
						rows="15" cols="80" title="All data type mappings from SyncLite to destination database. You can edit individual mappings by selecting 'Customize' option in the previous dropdown."><%=properties.get("dst-data-type-all-mappings" + dstIndex)%></textarea></td>
					</tr>
				</tbody>				
			</table>
			<center>
				<button type="submit" name="next">Next</button>
			</center>			
		</form>
	</div>
</body>
</html>
