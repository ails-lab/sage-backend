package ac.software.semantic.security;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.lang.Nullable;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

public class CustomAuthenticationFilter extends UsernamePasswordAuthenticationFilter {

    private static final String customToken = "jdjCustomToken";

    /**
     * This is override of UsernamePasswordAuthenticationFilter
     * @param request
     * @param response
     * @return
     * @throws AuthenticationException
     */
    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response) throws AuthenticationException {

        UsernamePasswordAuthenticationToken authRequest = getAuth(request);
        setDetails(request, authRequest);
        return getAuthenticationManager().authenticate(authRequest);
    }

    /*
        We are combing the username and our custom token to pass on this data to the underlying system.
        As an alternate, you can also save it in session but keep in mind that this will only be available
        where HTTP session is available.
     */
    private UsernamePasswordAuthenticationToken getAuth(final HttpServletRequest request) {
        String username = obtainUsername(request);
        String password = obtainPassword(request);
        String customToken = obtainCustomToken(request);

        String usernameDomain = String.format("%s%s%s", username.trim(),
            String.valueOf(Character.LINE_SEPARATOR), customToken);

        return new UsernamePasswordAuthenticationToken(
            usernameDomain, password);
    }

    @Nullable
    protected String obtainCustomToken(HttpServletRequest request) {
        return request.getParameter(customToken);
    }
}