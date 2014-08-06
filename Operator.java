package mappers;

public enum Operator {
	EQ(" = "), LE(" <= "), LT(" < "), GE(" >= "), GT(" > "), NEQ(" != "), LIKE(" % ");
	
	private String representacion;
	
	private Operator(String string){
		representacion = string;
	}
	
	@Override
	public String toString() {
		return representacion;
	}
}
