package ac.software.semantic.config;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.security.servlet.PathRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.BeanIds;

import ac.software.semantic.security.CustomUserDetailsService;
import ac.software.semantic.security.JwtAuthenticationEntryPoint;
import ac.software.semantic.security.JwtAuthenticationFilter;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(
        securedEnabled = true,
        jsr250Enabled = true,
        prePostEnabled = true
)
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {

	@Value("${frontend.server}")
    private String frontend;
	
	@Autowired
    CustomUserDetailsService customUserDetailsService;

    @Autowired
    private JwtAuthenticationEntryPoint unauthorizedHandler;

    @Bean
    public JwtAuthenticationFilter jwtAuthenticationFilter() {
        return new JwtAuthenticationFilter();
    }
    
    @Bean(BeanIds.AUTHENTICATION_MANAGER)
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }
    
	@Bean
	public PasswordEncoder passwordEncoder() {
	    return new BCryptPasswordEncoder();
	}

    @Override
    public void configure(AuthenticationManagerBuilder authenticationManagerBuilder) throws Exception {
        authenticationManagerBuilder
                .userDetailsService(customUserDetailsService)
                .passwordEncoder(passwordEncoder());
    }
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
    	
        http
        .formLogin()
            .usernameParameter("email") //define that instead of username field we will use email
            .and()
        .cors()
            .and()
        .csrf()
            .disable()
        .exceptionHandling()
            .authenticationEntryPoint(unauthorizedHandler)
            .and()
        .sessionManagement()
            .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            .and()
        .authorizeRequests()
            //Authorize all static resource and documentation requests
            .antMatchers("/",
                "/favicon.ico", "/**/*.png", "/**/*.gif", "/**/*.svg", "/**/*.jpg", "/**/*.html", "/**/*.css", "/**/*.js")
                .permitAll()
            .antMatchers("/api-docs", "/api-docs/**", "/swagger-ui/*")
                .permitAll()
            //Authorize login endpoint
            .antMatchers("/api/auth/**")
                .permitAll()
            //User resource authorization rules
            .antMatchers("/api/users/getPublicEditors", "/api/users/create")
                .permitAll()
            .antMatchers("/api/users/getValidatorsOfEditor", "/api/users/removeValidator")
                .hasAnyAuthority("SUPER", "EDITOR")
            //Access resource authorization rules
            .antMatchers("/api/access/getAll")
                .authenticated()
            .antMatchers("/api/access/**")
                .hasAnyAuthority("SUPER", "EDITOR")
            //Other rules
            .antMatchers("/api/data/**", "/api/mappings/**",  "/api/files/**", "/api/annotation/**", "/api/paged-annotation-validation/**", "/api/filter-annotation-validation/**", "/api/dataset/**", "/api/annotation-edit-group/**", "/api/vocabularizer/**", "/api/server/**", "/api/database/**", "/api/d2rml/**")
                .authenticated()
            .antMatchers("/api/mappings/**", "/api/users/**", "/api/files/**", "/api/annotation/**", "/api/paged-annotation-validation/**", "/api/filter-annotation-validation/**", "/api/dataset/**", "/api/annotation-edit-group/**", "/api/vocabularizer/**", "/api/server/**", "/api/database/**", "/api/d2rml/**")
                .permitAll()
            .antMatchers("/api/f/datasets/**", "/api/f/resources/**")
//                .authenticated()
                .permitAll()
            .antMatchers("/api/content/**")
                .permitAll()
            .anyRequest()
                .authenticated();

		// Add our custom JWT security filter
		http.addFilterBefore(jwtAuthenticationFilter(), UsernamePasswordAuthenticationFilter.class);    	
    }
    
    private final long MAX_AGE_SECS = 3600;
    
    @Bean
    CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
//        configuration.setAllowedOrigins(Arrays.asList(frontend));
        configuration.setAllowedOrigins(Arrays.asList("*"));
        configuration.setAllowedMethods(Arrays.asList("HEAD", "OPTIONS", "GET", "POST", "PUT", "PATCH", "DELETE"));
//        configuration.setAllowedMethods(Arrays.asList("*"));
//        configuration.setAllowedHeaders(Arrays.asList("authorization", "content-type"));
        configuration.setAllowedHeaders(Arrays.asList("*"));
//        configuration.setExposedHeaders(Arrays.asList("x-auth-token"));        
//        configuration.setExposedHeaders(Arrays.asList("*"));
        configuration.setMaxAge(MAX_AGE_SECS);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }    
}
