package ac.software.semantic.model;

public class Argument {
   private String name;
   
   private String[] parts;
   
   public Argument() { 
	   this(null, null);
   }
   
   public Argument(String name) {
	   this(name, null);
   }
   
   public Argument(String name, String[] parts) {
	   this.name = name;
	   this.parts = parts;
   }
   
   public String getName() {
	   return name;
   }
   
   public void setName(String name) {
	   this.name = name;
   }
   
   public String[] getParts() {
	   return parts;
   }
   
   public void setParts(String[] parts) {
   		this.parts = parts;
   }
}
