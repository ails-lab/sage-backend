package ac.software.semantic.payload;

import ac.software.semantic.controller.APIUserController;

public class ChangeUserDetailsRequest {
    APIUserController.UserDetailsUpdateOptions target;
    String value;
    String oldPassword;
    String newPassword;

    public APIUserController.UserDetailsUpdateOptions getTarget() {
        return target;
    }

    public void setTarget(APIUserController.UserDetailsUpdateOptions target) {
        this.target = target;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOldPassword() {
        return oldPassword;
    }

    public void setOldPassword(String oldPassword) {
        this.oldPassword = oldPassword;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }
}
