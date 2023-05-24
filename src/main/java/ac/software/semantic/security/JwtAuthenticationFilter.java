package ac.software.semantic.security;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import ac.software.semantic.model.constants.UserRoleType;
import ac.software.semantic.service.UserSessionService;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.Optional;

public class JwtAuthenticationFilter extends OncePerRequestFilter {

    @Autowired
    private JwtTokenProvider tokenProvider;

    @Autowired
    private CustomUserDetailsService customUserDetailsService;

    @Autowired
    private UserSessionService userSessionService;

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        try {
            String jwt = getJwtFromRequest(request);

            if (StringUtils.hasText(jwt) && tokenProvider.validateToken(jwt)) {
                String userId = tokenProvider.getUserIdFromJWT(jwt);
                
                UserDetails userDetails = customUserDetailsService.loadUserById(userId);
                
                /*
                 * The library we use for jwts on the issuedAt strips the miliseconds from the date, so the issued at date of the token 
                 * when the token is generated from the signin procedure is ~1000 miliseconds less than the
                 * lastLogin date of the user. This is why we check if the difference between the two is
                 * greater than 1000 miliseconds. If it is, then the user has logged in from another device
                 * and the token should be considered invalid.
                 */
                
                Optional<Date> lastLoginOpt = userSessionService.getLastLogin(new ObjectId(userId));
                if (lastLoginOpt.isPresent()) {
                	long lastLogin = lastLoginOpt.get().getTime();
                    long tokenIssuedAt = tokenProvider.getIssuedAtFromJWT(jwt).getTime();

                    if (lastLogin - tokenIssuedAt > 1000) {
                        throw new Exception("Token expired, User logged in from another device");
                    }
                }
            
                ((UserPrincipal)userDetails).setType(UserRoleType.valueOf(tokenProvider.getRoleFromJWT(jwt)));
                UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(userDetails, null, userDetails.getAuthorities());
                authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

                SecurityContextHolder.getContext().setAuthentication(authentication);
            }
        } catch (Exception ex) {
            logger.error("Could not set user authentication in security context", ex);
        }

        filterChain.doFilter(request, response);
    }

    private String getJwtFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7, bearerToken.length());
        }
        return null;
    }
}