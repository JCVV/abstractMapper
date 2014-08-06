package mappers;

public class QueryCondition {

	private String columnName;
	private Operator operator;
	private Object value;
	
	public QueryCondition(String columnName, Operator operator, Object value) {
		this.columnName = columnName;
		this.operator = operator;
		this.value = value;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public Operator getOperator() {
		return operator;
	}
	
	public Object getValue() {
		return value;
	}
}
