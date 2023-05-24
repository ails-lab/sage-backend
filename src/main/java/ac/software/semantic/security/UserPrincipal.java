package ac.software.semantic.security;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.UserRoleType;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class UserPrincipal implements UserDetails {
    private String id;

    private String email;
    private String uuid;

    @JsonIgnore
    private String password;
    
    private UserRoleType type;

    private Collection<? extends GrantedAuthority> authorities;
    
    public UserPrincipal(User user) {
        this.id = user.getId().toString();
        this.email = user.getEmail();
        this.password = user.getBCryptPassword();
        this.uuid =  user.getUuid();
        this.authorities = new ArrayList<>();
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
        if (type != null) {
        	authorities.add(new SimpleGrantedAuthority(type.toString()));
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

	public UserRoleType getType() {
		return type;
	}

	public void setType(UserRoleType type) {
		this.type = type;
	}
}
