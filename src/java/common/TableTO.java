package common;

public class TableTO {

	private String tableName;
	private String pkNames;
	private String pkTypes;
	private String columns;
	private String columnsType;
	private String parts;
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getPkNames() {
		return pkNames;
	}
	public void setPkNames(String pkNames) {
		this.pkNames = pkNames;
	}
	public String getPkTypes() {
		return pkTypes;
	}
	public void setPkTypes(String pkTypes) {
		this.pkTypes = pkTypes;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getColumnsType() {
		return columnsType;
	}
	public void setColumnsType(String columnsType) {
		this.columnsType = columnsType;
	}
	public String getParts() {
		return parts;
	}
	public void setParts(String parts) {
		this.parts = parts;
	}
	
}
