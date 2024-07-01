package ac.software.semantic.security;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.User;
import ac.software.semantic.model.UserRole;
import ac.software.semantic.model.UserRoleDefault;
import ac.software.semantic.model.constants.type.UserRoleType;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class UserPrincipal implements UserDetails {
    private String id;

    private String email;
    private String uuid;
    
    private String name;
    
    private Boolean multiLogin;

    @JsonIgnore
    private String password;
    
    private UserRoleType role;
    private List<UserRoleDefault> rolesDefault;
    
    private List<UserRoleType> roles;

    private Collection<? extends GrantedAuthority> authorities;
    
    public UserPrincipal(User user, UserRole ur) {//, Collection<? extends GrantedAuthority> authorities) {
        this.id = user.getId().toString();
        this.email = user.getEmail();
        this.password = user.getBCryptPassword();
        this.uuid =  user.getUuid();
        this.name = user.getName();
        this.authorities = new ArrayList<>();
        
        this.multiLogin = user.getMultiLogin();
        
        if (ur != null) {
        	this.roles = ur.getRole();
        	this.rolesDefault = ur.getRoleDefault();
        }
    }

    public String getId() {
        return id;
    }
    
    /*
        I had to override the getUsername method.
        Now it returns email.
        Seems strange but works.
    */
    @Override
    public String getUsername() {
        return email;
    }

    @Override
    public String getPassword() {
        return password;
    }
    
    public String getUuid() {
        return uuid;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        List<SimpleGrantedAuthority> authorities = new ArrayList<>();
        if (role != null) {
        	authorities.add(new SimpleGrantedAuthority(role.toString()));
        }
        return authorities;
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    public UserRoleDefault getUserRoleDefault() {
    	if (this.rolesDefault != null && this.role != null) {
    		for (UserRoleDefault urd : rolesDefault) {
    			if (urd.getRole() == role) {
    				return urd;
    			}
    		}
    	}
    	
    	return null;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserPrincipal that = (UserPrincipal) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id);
    }

	public UserRoleType getRole() {
		return role;
	}

	public void setRole(UserRoleType role) {
		this.role = role;
		
//		if (this.roles != null) {
//			for (UserRole )
//		}
	}


	public List<UserRoleType> getRoles() {
		return roles;
	}


	public void setRoles(List<UserRoleType> roles) {
		this.roles = roles;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Boolean getMultiLogin() {
		return multiLogin;
	}

	public void setMultiLogin(Boolean multiLogin) {
		this.multiLogin = multiLogin;
	}
	
	public boolean allowMultiLogin() {
		if (multiLogin == null) {
			return false;
		} else {
			return multiLogin;
		}
	}

//	public List<UserRoleDefault> getRolesDefault() {
//		return rolesDefault;
//	}

	public void setRolesDefault(List<UserRoleDefault> rolesDefault) {
		this.rolesDefault = rolesDefault;
	}
}
