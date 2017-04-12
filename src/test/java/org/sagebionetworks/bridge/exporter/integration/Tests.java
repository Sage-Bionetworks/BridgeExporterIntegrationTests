package org.sagebionetworks.bridge.exporter.integration;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;

import org.sagebionetworks.bridge.rest.Config;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.EmailTemplate;
import org.sagebionetworks.bridge.rest.model.MimeType;
import org.sagebionetworks.bridge.rest.model.Study;

public class Tests {
    
    private static final Config CONFIG = new Config();
    public static final String APP_NAME = "Integration Tests";
    public static final String TEST_KEY = "api";
    
    public static final EmailTemplate TEST_RESET_PASSWORD_TEMPLATE = new EmailTemplate().subject("Reset your password")
        .body("<p>${url}</p>").mimeType(MimeType.TEXT_HTML);
    public static final EmailTemplate TEST_VERIFY_EMAIL_TEMPLATE = new EmailTemplate().subject("Verify your email")
        .body("<p>${url}</p>").mimeType(MimeType.TEXT_HTML);

    public static ClientInfo getClientInfoWithVersion(String osName, int version) {
        return new ClientInfo().appName(APP_NAME).appVersion(version).deviceName(APP_NAME).osName(osName)
                .osVersion("2.0.0").sdkName("BridgeJavaSDK").sdkVersion(Integer.parseInt(CONFIG.getSdkVersion()));
    }

    public static String randomIdentifier(Class<?> cls) {
        return ("sdk-" + cls.getSimpleName().toLowerCase() + "-" + RandomStringUtils.randomAlphabetic(5)).toLowerCase();
    }

    public static String makeEmail(Class<?> cls) {
        String devName = CONFIG.getDevName();
        String clsPart = cls.getSimpleName();
        String rndPart = RandomStringUtils.randomAlphabetic(4);
        return String.format("bridge-testing+%s-%s-%s@sagebase.org", devName, clsPart, rndPart);
    }
    
    public static Study getStudy(String identifier, Long version) {
        Study study = new Study();
        study.setIdentifier(identifier);
        study.setMinAgeOfConsent(18);
        study.setName("Test Study [Exporter Integ Test]" + randomIdentifier(Tests.class));
        study.setSponsorName("The Test Study Folks [SDK]");
        study.setSupportEmail("test@test.com");
        study.setConsentNotificationEmail("test2@test.com");
        study.setTechnicalEmail("test3@test.com");
        study.setUsesCustomExportSchedule(true);
        study.getUserProfileAttributes().add("new_profile_attribute");
        study.setTaskIdentifiers(Lists.newArrayList("taskA")); // setting it differently just for the heck of it 
        study.setDataGroups(Lists.newArrayList("beta_users", "production_users"));
        study.setResetPasswordTemplate(Tests.TEST_RESET_PASSWORD_TEMPLATE);
        study.setVerifyEmailTemplate(Tests.TEST_VERIFY_EMAIL_TEMPLATE);
        study.setHealthCodeExportEnabled(Boolean.TRUE);
        study.setDisableExport(true);
        study.setUsesCustomExportSchedule(false);
        
        Map<String,Integer> map = new HashMap<>();
        map.put("Android", 10);
        map.put("iPhone OS", 14);
        study.setMinSupportedAppVersions(map);
        
        Map<String,String> platformMap = new HashMap<>();
        platformMap.put("Android", "arn:android:"+identifier);
        platformMap.put("iPhone OS", "arn:ios:"+identifier);
        study.setPushNotificationARNs(platformMap);
        
        if (version != null) {
            study.setVersion(version);
        }
        return study;
    }
}
