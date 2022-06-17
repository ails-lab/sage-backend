package ac.software.semantic.payload;
import io.swagger.v3.oas.annotations.media.Schema;

public class NewUserRequest {
    @Schema(description = "Email Used for user registration", 
    example = "foo@bar.com", required = true)
    private String email;
    @Schema(description = "Password chosen by user", 
    example = "123456", required = true)
    private String password;
    @Schema(description = "Name of new user", 
    example = "Nick Cave", required = true)
    private String name;
    @Schema(description = "Metadata - description of user - org", 
    example = "EUSCREEN", required = true)
    private String jobDescription;

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobDescription() {
        return jobDescription;
    }

    public void setjobDescription(String jobDescription) {
        this.jobDescription = jobDescription;
    }

}