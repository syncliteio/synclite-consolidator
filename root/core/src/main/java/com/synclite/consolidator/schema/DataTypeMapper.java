package com.synclite.consolidator.schema;

import java.sql.JDBCType;

import com.synclite.consolidator.global.ConfLoader;
import com.synclite.consolidator.global.DstDataTypeMapping;

public abstract class DataTypeMapper {

	protected int dstIndex;
    protected DataTypeMapper(int dstIndex) {
    	this.dstIndex = dstIndex;
    }

    public static DataTypeMapper getInstance(int dstIndex) {
        switch(ConfLoader.getInstance().getDstType(dstIndex)) {
        case APACHE_ICEBERG:
        	return new ApacheIcebergDataTypeMapper(dstIndex);
        case SQLITE:
    		return new SQLiteDataTypeMapper(dstIndex);
        case POSTGRESQL:
            return new PGDataTypeMapper(dstIndex);
        case MONGODB:
        	return new SQLiteDataTypeMapper(dstIndex);
        case DUCKDB:    		
        	return new DuckDBDataTypeMapper(dstIndex);
        case MYSQL:        	
        	return new MySQLDataTypeMapper(dstIndex);
        }
		return null;
    }

	protected DataType doMapTypeExact(DataType type) {
		return type;
	}

    protected abstract DataType doMapTypeConservative(DataType type);

    protected final DataType mapTypeForSystemColumn(DataType srcType) {
    	DataType mappedType = doMapTypeForSystemColumn(srcType);
    	return mappedType;
    }
    
    protected DataType doMapTypeForSystemColumn(DataType srcType) {
		return doMapTypeBestEffort(srcType);
	}

	protected final DataType mapType(DataType srcType) {
    	DataType mappedType = null;
    	if (ConfLoader.getInstance().getDstDataTypeMapping(dstIndex) == DstDataTypeMapping.CUSTOMIZED) {
    		mappedType = userMappedType(srcType);
    	}
        if (mappedType == null) {
        	if (ConfLoader.getInstance().getDstDataTypeMapping(dstIndex) == DstDataTypeMapping.EXACT) {
        		mappedType = doMapTypeExact(srcType);
        	} else if (ConfLoader.getInstance().getDstDataTypeMapping(dstIndex) == DstDataTypeMapping.BEST_EFFORT) {
                mappedType = doMapTypeBestEffort(srcType);
            } else {
                mappedType = doMapTypeConservative(srcType);
            }
        }
        return mappedType;
    }

    private final DataType userMappedType(DataType type) {
    	String typeToCheck = type.dbNativeDataType.replace("\\s+", "").toLowerCase();
        String propName = "map-src-" + typeToCheck + "-to-dst-" + dstIndex;
        String userMappedSqlDataType = ConfLoader.getInstance().getPropertyValue(propName);
        if (userMappedSqlDataType != null) {
        	return new DataType(userMappedSqlDataType, getJavaSqlType(userMappedSqlDataType), getStorageClass(userMappedSqlDataType));
        } else {
        	//check if this is a variable length/decimal type
        	if (typeToCheck.contains("(")) {
        		String baseSrcTypeName = typeToCheck.substring(0, typeToCheck.indexOf("(")).trim();
        		//Look for mapping
        		propName = "map-src-" + baseSrcTypeName + "(length)-to-dst-" + dstIndex;
                userMappedSqlDataType = ConfLoader.getInstance().getPropertyValue(propName);
                if (userMappedSqlDataType != null) {
                	userMappedSqlDataType = userMappedSqlDataType.toLowerCase();
                	if (userMappedSqlDataType.contains("(length)")) {
	                	String baseDstTypeName = userMappedSqlDataType.substring(0, userMappedSqlDataType.indexOf("(")).trim();
	                	
	                	//Just replace the baseTypeName and map
	                	String mappedTypeName = typeToCheck.replace(baseSrcTypeName, baseDstTypeName);
	                	return new DataType(mappedTypeName, getJavaSqlType(mappedTypeName), getStorageClass(mappedTypeName));
                	} else {
	                	//Just use the specified mapped type name
	                	return new DataType(userMappedSqlDataType, getJavaSqlType(userMappedSqlDataType), getStorageClass(userMappedSqlDataType));
                	}
                } else {
                	//check for decimal mappings
            		propName = "map-src-" + baseSrcTypeName + "(precision,scale)-to-dst-" + dstIndex;
                    userMappedSqlDataType = ConfLoader.getInstance().getPropertyValue(propName);
                    if (userMappedSqlDataType != null) {
                    	userMappedSqlDataType = userMappedSqlDataType.toLowerCase();
                    	if (userMappedSqlDataType.contains("(precision,scale)")) {
    	                	String baseDstTypeName = userMappedSqlDataType.substring(0, userMappedSqlDataType.indexOf("(")).trim();	                	
    	                	//Just replace the baseTypeName and map
    	                	String mappedTypeName = typeToCheck.replace(baseSrcTypeName, baseDstTypeName);
    	                	return new DataType(mappedTypeName, getJavaSqlType(mappedTypeName), getStorageClass(mappedTypeName));
    	                } else {
    	                	//Just use the specified mapped type name
    	                	return new DataType(userMappedSqlDataType, getJavaSqlType(userMappedSqlDataType), getStorageClass(userMappedSqlDataType));
    	                }
                    } else {
                    	//If no mapping found then map conservatively
                    	return doMapTypeConservative(type);
                    }               	
                }
        	} else {
        		return doMapTypeConservative(type);
        	}
        }
    }
    
    protected long getStringDataTypeLength(String type) {
/*
    	CHARACTER(20)
		VARCHAR(255)
		VARYING CHARACTER(255)
		NCHAR(55)
		NATIVE CHARACTER(70)
		NVARCHAR(100)
*/
		String[] tokens = type.split("\\(|\\)");
		if (tokens.length > 1) {
			Long len = Long.valueOf(tokens[tokens.length-1]);
			if (len != null) {
				return len;
			}
		}
		return -1;
    }
    
    protected long getDecimalDataTypePrecision(String type) {
    	String[] tokens = type.split("\\(|\\)");
		if (tokens.length > 1) {
			String[] subTokens = tokens[tokens.length-1].split(",");
			
			if (subTokens.length == 0) {
				return -1;
			} else {
				Long precision = Long.valueOf(subTokens[0]);
				if (precision != null) {
					return precision;
				}
			}
		}
		return -1;    	
    }

    protected long getDecimalDataTypeScale(String type) {
    	String[] tokens = type.split("\\(|\\)");
		if (tokens.length > 1) {
			String[] subTokens = tokens[tokens.length-1].split(",");
			
			if (subTokens.length == 0) {
				return -1;
			} else {
				if (subTokens.length == 2) {
					Long scale = Long.valueOf(tokens[1]);
					if (scale != null) {
						return scale;
					}
				}
			}
		}
		return -1;  	
    }

    protected DataType doMapTypeBestEffort(DataType type) {
        String typeToCheck = type.dbNativeDataType.toLowerCase().trim().split("[\\s(]+")[0];
        switch(typeToCheck) {
        case "smallserial" :
        case "serial" :
        case "bigserial" :	
        case "bit" :
        case "integer" :
        case "int" :
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "bigint":        	
        case "int2":
        case "int4":	
        case "int8":
        case "long":
        case "byteint":	
        case "unsigned":
        	return getBestEffortIntegerDataType();
        case "text":
        case "varchar":
        case "varchar2":
        case "nvarchar":
        case "char":
        case "nchar":
        case "native":
        case "character":
        case "varying":
        case "nvarchar2":	
        case "xmltype" :
        case "xml":
        case "json":
        	return getBestEffortTextDataType();
        case "array":
		case "integer[]":
		case "bigint[]":
		case "text[]":
		case "boolean[]":
		case "float[]":
		case "numeric[]":
		case "timestamp[]":
		case "date[]":
		case "time[]":
		case "character[]":
		case "json[]":
		case "jsonb[]":
		case "vector":	
        	//return new DataType("text", JDBCType.CLOB, StorageClass.TEXT);
        	return getBestEffortArrayDataType(type);
        case "clob":
        case "dbclob":	
        	return getBestEffortClobDataType();
        case "blob":
        case "bytea":	
        case "binary":
        case "varbinary":
        case "image":
        case "object":
        case "geography" :
        case "geometry" :
        case "raw" :	
        case "sdo_geometry" :
        case "sdo_topo_geometry" :	
        case "bfile" :	
        case "ref" :
        case "ordicom" :
        case "ordaudio" :
        case "ordvideo" :
        case "orddoc" :
        case "table" :	
        case "associative":
        case "varray":
        case "graphic":
        case "vargraphic":	
        	return getBestEffortBlobDataType();
        case "real":
        case "double":
        case "float":
        case "numeric":
        case "money":
        case "smallmoney":
        case "number":
        case "decimal":
        case "binary_float":
        case "binary_double":
            //return new DataType("numeric", JDBCType.NUMERIC, StorageClass.NUMERIC);
            return getBestEffortRealDataType();
        case "boolean":
        case "bool":
        	//return new DataType("boolean", JDBCType.BOOLEAN, StorageClass.NUMERIC);
        	return getBestEffortBooleanDataType();
        case "date":
            //return new DataType("date", JDBCType.VARCHAR, StorageClass.TEXT);
        	return getBestEffortDateDataType();
        case "datetime":
        case "datetime2":        	
        case "time":     	
        case "timestamp":	
            //return new DataType("timestamp", JDBCType.VARCHAR, StorageClass.TEXT);
            return getBestEffortDateTimeDataType();
        }
        
        return getBestEffortTextDataType();
    }

	protected DataType getBestEffortArrayDataType(DataType t) {
		return getBestEffortTextDataType();
	}

	protected final StorageClass getStorageClass(String type) {
        String typeToCheck = type.toLowerCase().trim().split("[\\s(]+")[0];
        switch(typeToCheck) {
        case "smallserial" :
        case "serial" :
        case "bigserial" :	
        case "bit" :
        case "integer" :
        case "int" :
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "bigint":        	
        case "int2":
        case "int4":	
        case "int8":
        case "long":
        case "byteint":	
        case "unsigned":
        	return StorageClass.INTEGER;
        case "text":
        case "string":	
        case "varchar":
        case "varchar2":	
        case "nvarchar":
        case "char":
        case "nchar":
        case "native":        	
        case "character":
        case "varying":
        case "nvarchar2":
        case "xmltype":
        case "xml":
        case "json":	
		case "array":
		case "integer[]":
		case "bigint[]":
		case "text[]":
		case "boolean[]":
		case "float[]":
		case "numeric[]":
		case "timestamp[]":
		case "date[]":
		case "time[]":
		case "character[]":
		case "json[]":
		case "jsonb[]":
		case "vector":	
        	//return new DataType("text", JDBCType.CLOB, StorageClass.TEXT);
        	return StorageClass.TEXT;
        case "clob":
        case "dbclob":	
        	return StorageClass.CLOB;
        case "blob":
        case "bytea":	
        case "binary":
        case "varbinary":
        case "image":
        case "object":
        case "geography" :
        case "geometry" :
        case "raw" :	
        case "sdo_geometry" :
        case "sdo_topo_geometry" :	
        case "bfile" :	
        case "ref" :
        case "ordicom" :
        case "ordaudio" :
        case "ordvideo" :
        case "orddoc" :
        case "table" :	
        case "associative":
        case "varray":
        case "graphic":
        case "vargraphic":	
            //return new DataType("bytea", JDBCType.BLOB, StorageClass.BLOB);
        	return StorageClass.BLOB;
        case "real":
        case "double":
        case "float":
        case "numeric":
        case "money":
        case "smallmoney":
        case "number":
        case "decimal":
        case "binary_float":
        case "binary_double":	
            //return new DataType("numeric", JDBCType.NUMERIC, StorageClass.NUMERIC);
            return StorageClass.REAL;
        case "bool":
        case "boolean":
            //return new DataType("boolean", JDBCType.BOOLEAN, StorageClass.NUMERIC);
        	return StorageClass.INTEGER;
        case "date":
            //return new DataType("date", JDBCType.VARCHAR, StorageClass.TEXT);
        	return StorageClass.DATE;
        case "datetime":
        case "datetime2":        	
        case "timestamp":	
            //return new DataType("timestamp", JDBCType.VARCHAR, StorageClass.TEXT);
            return StorageClass.TIMESTAMP;
        case "time":     	
        	return StorageClass.TIME;
        }        
        return StorageClass.TEXT;
    }

	protected final JDBCType getJavaSqlType(String type) {
        String typeToCheck = type.toLowerCase().trim().split("[\\s(]+")[0];
        switch(typeToCheck) {
        case "smallserial" :
        	return JDBCType.SMALLINT;
        case "serial" :
        	return JDBCType.INTEGER;
        case "bigserial" :	
        	return JDBCType.BIGINT;
        case "bit" :
        	return JDBCType.BIT;
        case "integer" :
        	return JDBCType.INTEGER;
        case "int" :
        	return JDBCType.INTEGER;
        case "tinyint":
        	return JDBCType.TINYINT;
        case "smallint":
        	return JDBCType.SMALLINT;
        case "mediumint":
        	return JDBCType.INTEGER;
        case "bigint":
        	return JDBCType.BIGINT;
        case "int2":
        	return JDBCType.SMALLINT;
        case "int4":	
        	return JDBCType.INTEGER;
        case "int8":
        	return JDBCType.BIGINT;
        case "long":
        	return JDBCType.BIGINT;
        case "byteint":	
        	return JDBCType.TINYINT;
        case "unsigned":
        	return JDBCType.INTEGER;
        case "text":
        	return JDBCType.VARCHAR;
        case "clob":
        case "dbclob":	
        	return JDBCType.CLOB;
        case "varchar":
        	return JDBCType.VARCHAR;
        case "varchar2":	
        	return JDBCType.VARCHAR;
        case "nvarchar":
        	return JDBCType.NVARCHAR;
        case "nvarchar2":
        	return JDBCType.NVARCHAR;
        case "char":
        	return JDBCType.CHAR;
        case "nchar":
        	return JDBCType.NCHAR;
        case "native":        	
        	return JDBCType.NCHAR;
        case "character":
        	return JDBCType.CHAR;
        case "varying":	
        	return JDBCType.VARCHAR;
        case "xmltype":
        case "xml":
        case "json":	
        	return JDBCType.VARCHAR;
        case "blob":        	
        case "bytea":	
        case "binary":
        case "varbinary":
        case "image":
        case "object":
        case "geography" :
        case "geometry" :
        case "raw" :	
        case "sdo_geometry" :
        case "sdo_topo_geometry" :	
        case "bfile" :	
        case "ref" :
        case "ordicom" :
        case "ordaudio" :
        case "ordvideo" :
        case "orddoc" :
        case "table" :	
        case "associative":
        case "varray": 	
        case "graphic":
        case "vargraphic":	
        	return JDBCType.BLOB;
        case "real":
        	return JDBCType.REAL;
        case "double":
        case "binary_double":	
        	return JDBCType.DOUBLE;
        case "float":
        case "binary_float":	
        	return JDBCType.FLOAT;
        case "numeric":
        	return JDBCType.NUMERIC;
        case "money":
        	return JDBCType.REAL;
        case "smallmoney":
        	return JDBCType.REAL;
        case "number":
        	return JDBCType.NUMERIC;
        case "decimal":	
        	return JDBCType.DECIMAL;
        case "bool":
        case "boolean":
        	return JDBCType.BOOLEAN;
        case "date":
        	return JDBCType.DATE;
        case "datetime":        	
        case "datetime2":        	
        case "timestamp":
        	return JDBCType.TIMESTAMP;
        case "time":     	
        	return JDBCType.TIME;
		case "array":
		case "integer[]":
		case "bigint[]":
		case "text[]":
		case "boolean[]":
		case "float[]":
		case "numeric[]":
		case "timestamp[]":
		case "date[]":
		case "time[]":
		case "character[]":
		case "json[]":
		case "jsonb[]":
		case "vector":	
			return JDBCType.ARRAY;
        }       
        return JDBCType.VARCHAR;
	} 
    
	protected abstract DataType getBestEffortDateTimeDataType();

	protected abstract DataType getBestEffortDateDataType();

	protected abstract DataType getBestEffortBooleanDataType();

	protected abstract DataType getBestEffortBlobDataType();

	protected abstract DataType getBestEffortTextDataType();

	protected abstract DataType getBestEffortClobDataType();

	protected abstract DataType getBestEffortIntegerDataType();

	protected abstract DataType getBestEffortRealDataType();
    
}
