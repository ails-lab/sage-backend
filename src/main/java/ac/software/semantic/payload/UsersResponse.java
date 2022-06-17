package ac.software.semantic.payload;

import java.util.List;

public class UsersResponse {
    private List<NewUserSummary> users;
    
    public List<NewUserSummary> getUsers() {
        return users;
    }

    public UsersResponse(List<NewUserSummary> users) {
        this.users = users;
    }

    public void setUsers( List<NewUserSummary> users) {
        this.users = users;
    }
}