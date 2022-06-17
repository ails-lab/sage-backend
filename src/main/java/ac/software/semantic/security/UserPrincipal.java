package ac.software.semantic.security;

import com.fasterxml.jackson.annotation.JsonIgnore;

import ac.software.semantic.model.User;
import ac.software.semantic.model.UserType;

import org.bson.types.ObjectId;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class UserPrincipal implements UserDetails {
    private String id;

//    private String name;

    private String email;
    private String uuid;

//    @JsonIgnore
//    private String email;

    @JsonIgnore
    private String password;
    
    private UserType type;

    private Collection<? extends GrantedAuthority> authorities;

    public UserPrincipal(String id, String email, String password, String uuid, UserType type) {//, Collection<? extends GrantedAuthority> authorities) {
        this.id = id;
        this.email = email;
        this.password = password;
        this.uuid = uuid;
        this.type = type;
//        this.authorities = authorities;
        this.authorities = new ArrayList<>();
    }

    public static UserPrincipal create(User user) {
//        List<GrantedAuthority> authorities = user.getRoles().stream().map(role ->
//                new SimpleGrantedAuthority(role.getName().name())
//        ).collect(Collectors.toList());

        return new UserPrincipal(
                user.getId().toString(),
                user.getEmail(),
                user.getBCryptPassword(),
                user.getUuid(),
                user.getType()
//                authorities
        );
    }

    public String getId() {
        return id;
    }

//    public String getName() {
//        return name;
//    }
//
//    public String getEmail() {
//        return email;
//    }
    
    /*
        I had to override the getUsername method.
        Now it returns email.
        Seems strange but works.
    */
    @Override
    public String getUsername() {
        return email;
    }

    // @Override
    // public String getEmail() {
    //     return email;
    // }

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
        authorities.add(new SimpleGrantedAuthority(type.toString()));
//        System.out.println(authorities);
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

	public UserType getType() {
		return type;
	}

	public void setType(UserType type) {
		this.type = type;
	}
}
