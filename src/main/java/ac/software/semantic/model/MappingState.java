package ac.software.semantic.model;

public enum MappingState {
	
	NOT_EXECUTED,
	EXECUTED,
	EXECUTING,
	EXECUTION_FAILED;
	
	public static MappingState get(String type) {
		if (type.equals("NOT_EXECUTED")) {
			return NOT_EXECUTED;
		} else if (type.equals("EXECUTED")) {
			return EXECUTED;
		} else if (type.equals("EXECUTING")) {
			return EXECUTING;
		} else if (type.equals("EXECUTION_FAILED")) {
			return EXECUTION_FAILED;
		} 
		
		return null;
	}
}
