package org.sagebionetworks.bridge.exporter.integration;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.Config;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.UserSessionInfo;

public class TestUserHelper {
    private static final List<String> LANGUAGES = Lists.newArrayList("en");
    private static final String PASSWORD = "P4ssword!";
    private static final Config CONFIG = new Config();

    private static final ClientInfo CLIENT_INFO = new ClientInfo();
    static {
        CLIENT_INFO.setAppName("BridgeEX Integration Tests");
        CLIENT_INFO.setAppVersion(1);
    }

    public static class TestUser {
        private SignIn signIn;
        private ClientManager manager;
        private UserSessionInfo userSession;

        public TestUser(SignIn signIn, ClientManager manager) {
            checkNotNull(signIn.getStudy());
            checkNotNull(signIn.getEmail());
            checkNotNull(signIn.getPassword());
            checkNotNull(manager);
            this.signIn = signIn;
            this.manager = manager;
        }
        public <T> T getClient(Class<T> service) {
            return manager.getClient(service);
        }
        public UserSessionInfo signInAgain() {
            AuthenticationApi authApi = manager.getClient(AuthenticationApi.class);
            try {
                userSession = authApi.signIn(getSignIn()).execute().body();
            } catch (ConsentRequiredException e) {
                userSession = e.getSession();
                throw e;
            } catch(IOException ioe) {
                throw new BridgeSDKException(ioe.getMessage(), ioe);
            }
            return userSession;
        }
        public void signOut() throws IOException {
            AuthenticationApi authApi = manager.getClient(AuthenticationApi.class);
            authApi.signOut().execute();
            userSession.setAuthenticated(false);
        }
        public void signOutAndDeleteUser() throws IOException {
            this.signOut();

            Config config = new Config();
            ClientManager adminManager = new ClientManager.Builder().withSignIn(config.getAdminSignIn())
                    .withConfig(config).withClientInfo(manager.getClientInfo()).withAcceptLanguage(LANGUAGES).build();
            ForAdminsApi adminsApi = adminManager.getClient(ForAdminsApi.class);
            adminsApi.deleteUser(userSession.getId()).execute();
        }
        public SignIn getSignIn() {
            return signIn;
        }
        public Config getConfig() {
            return manager.getConfig();
        }
        public UserSessionInfo getUserSession() {
            return userSession;
        }
    }

    public static TestUser getSignedInAdmin() {
        Config config = new Config();
        ClientManager adminManager = new ClientManager.Builder().withSignIn(config.getAdminSignIn())
                .withConfig(config).withClientInfo(CLIENT_INFO).withAcceptLanguage(LANGUAGES).build();
        TestUser adminUser = new TestUser(config.getAdminSignIn(), adminManager);
        adminUser.signInAgain();
        return adminUser;
    }

    public static TestUser createAndSignInUser(Class<?> cls, String studyId, boolean consentUser, Role... roles)
            throws IOException {
        return new TestUserHelper.Builder(cls).withRoles(roles).withConsentUser(consentUser).createAndSignInUser(
                studyId);
    }

    public static class Builder {
        private Class<?> cls;
        private boolean consentUser;
        private SignUp signUp;
        private ClientInfo clientInfo;
        private Set<Role> roles = new HashSet<>();
        
        public Builder withConsentUser(boolean consentUser) {
            this.consentUser = consentUser;
            return this;
        }

        public Builder withRoles(Role...roles) {
            Collections.addAll(this.roles, roles);
            return this;
        }
        
        public Builder withSignUp(SignUp signUp) {
            this.signUp = signUp;
            return this;
        }
        
        public Builder(Class<?> cls) {
            checkNotNull(cls);
            this.cls = cls;
        }

        public TestUser createAndSignInUser(String studyId) throws IOException {
            if (clientInfo == null) {
                clientInfo = CLIENT_INFO;
            }
            TestUser admin = getSignedInAdmin();
            ForAdminsApi adminsApi = admin.getClient(ForAdminsApi.class);

            Set<Role> rolesList = new HashSet<>();
            if (signUp != null && signUp.getRoles() != null) {
                rolesList.addAll(signUp.getRoles());
            }
            if (!roles.isEmpty()) {
                rolesList.addAll(roles);
            }

            // For email address, we don't want consent emails to bounce or SES will get mad at us. All test user email
            // addresses should be in the form bridge-testing+[semi-unique token]@sagebase.org. This directs all test
            // email to bridge-testing@sagebase.org.
            String emailAddress = makeEmail(cls);

            if (signUp == null) {
                signUp = new SignUp();
            }
            if (signUp.getEmail() == null) {
                signUp.email(emailAddress);
            }
            signUp.setStudy(studyId);
            signUp.setRoles(new ArrayList<>(rolesList));
            signUp.setPassword(PASSWORD);
            signUp.setConsent(consentUser);
            adminsApi.createUserInStudy(studyId, signUp).execute().body();

            SignIn signIn = new SignIn().study(signUp.getStudy()).email(signUp.getEmail())
                    .password(signUp.getPassword());
            
            ClientManager manager = new ClientManager.Builder().withConfig(admin.getConfig()).withSignIn(signIn)
                    .withClientInfo(clientInfo).withAcceptLanguage(LANGUAGES).build();
            TestUser testUser = new TestUser(signIn, manager);

            UserSessionInfo userSession = null;
            try {
                try {
                    userSession = testUser.signInAgain();
                } catch (ConsentRequiredException e) {
                    userSession = e.getSession();
                    if (consentUser) {
                        // If there's no consent but we're expecting one, that's an error.
                        throw e;
                    }
                }
                return testUser;
            } catch (RuntimeException ex) {
                // Clean up the account, so we don't end up with a bunch of leftover accounts.
                if (userSession != null) {
                    adminsApi.deleteUser(userSession.getId()).execute();
                }
                throw new BridgeSDKException(ex.getMessage(), ex);
            }
        }
    }

    public static String makeEmail(Class<?> cls) {
        String devName = CONFIG.getDevName();
        String clsPart = cls.getSimpleName();
        String rndPart = RandomStringUtils.randomAlphabetic(4);
        return String.format("bridge-testing+%s-%s-%s@sagebase.org", devName, clsPart, rndPart);
    }
}
