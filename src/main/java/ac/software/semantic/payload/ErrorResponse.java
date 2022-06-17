package ac.software.semantic.payload;
import io.swagger.v3.oas.annotations.media.Schema;

public class ErrorResponse {
    @Schema(description = "Message")
    private String message;

    public ErrorResponse(String message) {
        this.message = message;
    }

    public String getMessage() {
        return this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}