package org.sagebionetworks.bridge.exporter.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Team;

import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.HealthDataRecord;
import org.sagebionetworks.bridge.rest.model.RecordExportStatusRequest;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.SynapseExporterStatus;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;

@Category(IntegrationSmokeTest.class)
public class InstantExportTest {
    private static final String TEST_FILES_A = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"Yes\"],\n" +
            "  \"startDate\":\"2015-04-02T03:26:57-07:00\",\n" +
            "  \"questionTypeName\":\"SingleChoice\",\n" +
            "  \"item\":\"AAA\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";

    private static final String TEST_FILES_B = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"fencing\", \"running\", 3],\n" +
            "  \"startDate\":\"2015-04-02T03:26:57-07:00\",\n" +
            "  \"questionTypeName\":\"MultipleChoice\",\n" +
            "  \"item\":\"BBB\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";

    private static final int UPLOAD_STATUS_DELAY_MILLISECONDS = 5000;

    // Retry up to 6 times, so we don't spend more than 30 seconds per test.
    private static final int UPLOAD_STATUS_DELAY_RETRIES = 6;

    private static final String USER_NAME = "synapse.user";
    private static final String SYNAPSE_API_KEY_NAME = "synapse.api.key";
    private static final String EXPORTER_SYNAPSE_USER_ID_NAME = "exporter.synapse.user.id";
    private static final String TEST_USER_ID_NAME = "test.synapse.user.id";

    private static String SYNAPSE_USER;
    private static String SYNAPSE_API_KEY;
    private static String EXPORTER_SYNAPSE_USER_ID;
    private static Long TEST_USER_ID; // test user exists in synapse
    private static final String CONFIG_FILE = "BridgeExporter-test.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    private static final long MAX_PAGE_SIZE = 100L;
    private static TestUserHelper.TestUser worker;
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser researcher;
    private static TestUserHelper.TestUser user;

    private TestUserHelper.TestUser admin;
    private String studyId;
    private SynapseClient synapseClient;
    private Project project;
    private Team team;

    @Before
    public void before() throws IOException {
        setupProperties();

        admin = TestUserHelper.getSignedInAdmin();
        synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUserName(SYNAPSE_USER);
        synapseClient.setApiKey(SYNAPSE_API_KEY);

        // create test study
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);

        studyId = Tests.randomIdentifier(InstantExportTest.class);
        Study study = Tests.getStudy(studyId, null);

        studiesApi.createStudy(study).execute().body();
        Study newStudy = studiesApi.getStudy(study.getIdentifier()).execute().body();
        study.addDataGroupsItem("test_user"); // added by the server, required for equality of dataGroups.

        // then create test users in this study
        worker = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,false, Role.WORKER);
        developer = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,false, Role.DEVELOPER);
        researcher = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,true, Role.RESEARCHER);
        user = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,true);

        // create test files
        JsonNode AAA = DefaultObjectMapper.INSTANCE.readTree(TEST_FILES_A);
        JsonNode BBB = DefaultObjectMapper.INSTANCE.readTree(TEST_FILES_B);

        ObjectNode info1 = DefaultObjectMapper.INSTANCE.createObjectNode();
        info1.put("filename", "AAA.json");
        info1.put("timestamp", DateTime.now().toString());

        ObjectNode info2 = DefaultObjectMapper.INSTANCE.createObjectNode();
        info2.put("filename", "BBB.json");
        info2.put("timestamp", DateTime.now().toString());

        ObjectNode infoGeneral = DefaultObjectMapper.INSTANCE.createObjectNode();
        ArrayNode arrayNode = DefaultObjectMapper.INSTANCE.createArrayNode();
        arrayNode.add(info1);
        arrayNode.add(info2);
        infoGeneral.set("files", arrayNode);
        infoGeneral.put("item", "legacy-survey");
        infoGeneral.put("schemaRevision", 1);
        infoGeneral.put("appVersion", "version 1.0.0, build 1");
        infoGeneral.put("phoneInfo", "Integration Tests");

        try (FileWriter file = new FileWriter("./AAA.json")) {
            file.write(AAA.toString());
            System.out.println("Successfully Copied JSON Object to File...");
            System.out.println("\nJSON Object: " + AAA);
        }

        try (FileWriter file = new FileWriter("./BBB.json")) {
            file.write(BBB.toString());
            System.out.println("Successfully Copied JSON Object to File...");
            System.out.println("\nJSON Object: " + BBB);
        }

        try (FileWriter file = new FileWriter("./info.json")) {
            file.write(infoGeneral.toString());
            System.out.println("Successfully Copied JSON Object to File...");
            System.out.println("\nJSON Object: " + infoGeneral);
        }

        // zip
        String zipFile = "./legacy-survey.zip";
        String[] srcFiles = {"./AAA.json","./BBB.json","./info.json"};
        try {
            byte[] buffer = new byte[1024];

            FileOutputStream fos = new FileOutputStream(zipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);

            for (String file : srcFiles) {
                File srcFile = new File(file);
                FileInputStream fis = new FileInputStream(srcFile);

                zos.putNextEntry(new ZipEntry(srcFile.getName()));

                int length;
                while ((length = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, length);
                }

                zos.closeEntry();
                fis.close();
            }

            zos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // encrypt it
        String command = "activator \"run-main org.sagebionetworks.bridge.util.UploadArchiveUtil " +
                "encrypt " + "api" + " ./legacy-survey.zip ./legacy-survey-encrypted\"";

        String output = executeCommand(command);

        System.out.println(output);
    }

    @After
    public void after() throws Exception {
        if (studyId != null) {
            admin.getClient(StudiesApi.class).deleteStudy(studyId, true).execute();
        }
        if (project != null) {
            synapseClient.deleteEntityById(project.getId());
        }
        if (team != null) {
            synapseClient.deleteTeam(team.getId());
        }
        admin.signOut();
    }

    private String executeCommand(String command) {

        StringBuffer output = new StringBuffer();

        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine())!= null) {
                output.append(line + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return output.toString();

    }

    private org.sagebionetworks.bridge.config.Config bridgeEXIntegTestConfig() throws IOException {
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        if (Files.exists(localConfigPath)) {
            return new PropertiesConfig(DEFAULT_CONFIG_FILE, localConfigPath);
        } else {
            return new PropertiesConfig(DEFAULT_CONFIG_FILE);
        }
    }

    private void setupProperties() throws IOException {
        org.sagebionetworks.bridge.config.Config config = bridgeEXIntegTestConfig();

        SYNAPSE_USER = config.get(USER_NAME);
        SYNAPSE_API_KEY = config.get(SYNAPSE_API_KEY_NAME);
        EXPORTER_SYNAPSE_USER_ID = config.get(EXPORTER_SYNAPSE_USER_ID_NAME);
        TEST_USER_ID = Long.parseLong(config.get(TEST_USER_ID_NAME));
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        // developer is to ensure schemas exist. user is to do uploads
        worker = TestUserHelper.createAndSignInUser(InstantExportTest.class, false, Role.WORKER);
        developer = TestUserHelper.createAndSignInUser(InstantExportTest.class, false, Role.DEVELOPER);
        researcher = TestUserHelper.createAndSignInUser(InstantExportTest.class, true, Role.RESEARCHER);
        user = TestUserHelper.createAndSignInUser(InstantExportTest.class, true);

        // ensure schemas exist, so we have something to upload against
        UploadSchemasApi uploadSchemasApi = developer.getClient(UploadSchemasApi.class);

        UploadSchema legacySurveySchema = null;
        try {
            legacySurveySchema = uploadSchemasApi.getMostRecentUploadSchema("legacy-survey").execute().body();
        } catch (EntityNotFoundException ex) {
            // no-op
        }
        if (legacySurveySchema == null) {
            UploadFieldDefinition def1 = new UploadFieldDefinition();
            def1.setName("AAA");
            def1.setType(UploadFieldType.SINGLE_CHOICE);

            UploadFieldDefinition def2 = new UploadFieldDefinition();
            def2.setName("BBB");
            def2.setAllowOtherChoices(Boolean.FALSE);
            def2.setType(UploadFieldType.MULTI_CHOICE);
            def2.setMultiChoiceAnswerList(Lists.newArrayList("fencing", "football", "running", "swimming", "3"));

            legacySurveySchema = new UploadSchema();
            legacySurveySchema.setSchemaId("legacy-survey");
            legacySurveySchema.setRevision(1L);
            legacySurveySchema.setName("Legacy (RK/AC) Survey");
            legacySurveySchema.setSchemaType(UploadSchemaType.IOS_SURVEY);
            legacySurveySchema.setFieldDefinitions(Lists.newArrayList(def1,def2));
            uploadSchemasApi.createUploadSchema(legacySurveySchema).execute();
        }

        UploadSchema legacyNonSurveySchema = null;
        try {
            legacyNonSurveySchema = uploadSchemasApi.getMostRecentUploadSchema("legacy-non-survey").execute().body();
        } catch (EntityNotFoundException ex) {
            // no-op
        }
        if (legacyNonSurveySchema == null) {
            // Field types are already tested in UploadHandlersEndToEndTest in BridgePF unit tests. Don't need to
            // exhaustively test all field types, just a few representative ones: non-JSON attachment, JSON attachment,
            // attachment in JSON record, v1 type (string), v2 type (time)
            UploadFieldDefinition def1 = new UploadFieldDefinition();
            def1.setName("CCC.txt");
            def1.setType(UploadFieldType.ATTACHMENT_V2);
            UploadFieldDefinition def2 = new UploadFieldDefinition();
            def2.setName("FFF.json");
            def2.setType(UploadFieldType.ATTACHMENT_V2);
            UploadFieldDefinition def3 = new UploadFieldDefinition();
            def3.setName("record.json.HHH");
            def3.setType(UploadFieldType.ATTACHMENT_V2);
            UploadFieldDefinition def4 = new UploadFieldDefinition();
            def4.setName("record.json.PPP");
            def4.setType(UploadFieldType.STRING);
            UploadFieldDefinition def5 = new UploadFieldDefinition();
            def5.setName("record.json.QQQ");
            def5.setType(UploadFieldType.TIME_V2);

            legacyNonSurveySchema = new UploadSchema();
            legacyNonSurveySchema.setSchemaId("legacy-non-survey");
            legacyNonSurveySchema.setRevision(1L);
            legacyNonSurveySchema.setName("Legacy (RK/AC) Non-Survey");
            legacyNonSurveySchema.setSchemaType(UploadSchemaType.IOS_DATA);
            legacyNonSurveySchema.setFieldDefinitions(Lists.newArrayList(def1,def2,def3,def4,def5));
            uploadSchemasApi.createUploadSchema(legacyNonSurveySchema).execute();
        }
    }

    @AfterClass
    public static void deleteWorker() throws Exception {
        if (worker != null) {
            worker.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteDeveloper() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteResearcher() throws Exception {
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @AfterClass
    public static void deleteUser() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void test() throws Exception {
        // first, create a new study
        //StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        //
        //studyId = Tests.randomIdentifier(InstantExportTest.class);
        //Study study = Tests.getStudy(studyId, null);
        //assertNull("study version should be null", study.getVersion());
        //
        //VersionHolder holder = studiesApi.createStudy(study).execute().body();
        //assertNotNull(holder.getVersion());
        //Study newStudy = studiesApi.getStudy(study.getIdentifier()).execute().body();
        //study.addDataGroupsItem("test_user"); // added by the server, required for equality of dataGroups.
        //
        //// then create test users in this study
        //worker = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,false, Role.WORKER);
        //developer = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,false, Role.DEVELOPER);
        //researcher = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,true, Role.RESEARCHER);
        //user = TestUserHelper.createAndSignInUser(InstantExportTest.class, studyId,true);
        //
        //// then upload test data
        //testUpload("legacy-survey-encrypted");

        // then create test synapse project and team

        // then add test synapse table name into metadata ddb table

        // then execute instant exporting against this new study, done by researcher/developer

        // verification

        // finally, delete study (will delete study info in ExportTime table as well), synapse project and team and fields in SynapseMetaTables
    }

    private static void testUpload(String fileLeafName) throws Exception {
        // set up request
        String filePath = resolveFilePath(fileLeafName);
        File file = new File(filePath);

        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        UploadSession session = RestUtils.upload(usersApi, file);

        String uploadId = session.getId();

        // get validation status
        UploadValidationStatus status = null;
        for (int i = 0; i < UPLOAD_STATUS_DELAY_RETRIES; i++) {
            Thread.sleep(UPLOAD_STATUS_DELAY_MILLISECONDS);

            status = usersApi.getUploadStatus(session.getId()).execute().body();
            if (status.getStatus() == UploadStatus.VALIDATION_FAILED) {
                // Short-circuit. Validation failed. No need to retry.
                fail("Upload validation failed, UploadId=" + uploadId);
            } else if (status.getStatus() == UploadStatus.SUCCEEDED) {
                break;
            }
        }
        // userClient.upload marks the download complete
        // marking an already completed download as complete again should succeed (and be a no-op)
        worker.getClient(ForWorkersApi.class).completeUploadSession(session.getId());

        assertNotNull("Upload status is not null, UploadId=" + uploadId, status);
        assertEquals("Upload succeeded, UploadId=" + uploadId, UploadStatus.SUCCEEDED, status.getStatus());
        assertTrue("Upload has no validation messages, UploadId=" + uploadId, status.getMessageList().isEmpty());

        // Test some basic record properties.
        HealthDataRecord record = status.getRecord();
        assertEquals(uploadId, record.getUploadId());
        assertNotNull(record.getId());

        // For createdOn and createdOnTimeZone, these exist in the test files, but are kind of all over the place. For
        // now, just verify that the createdOn exists and that createdOnTimeZone can be parsed as a timezone as part of
        // a date.
        assertNotNull(record.getCreatedOn());
        assertNotNull(DateTime.parse("2017-01-25T16:36" + record.getCreatedOnTimeZone()));

        // check for update record export status, no need to activate exporter in testing
        RecordExportStatusRequest statusRequest = new RecordExportStatusRequest();
        statusRequest.setRecordIds(ImmutableList.of(record.getId()));
        statusRequest.setSynapseExporterStatus(SynapseExporterStatus.NOT_EXPORTED);
        worker.getClient(ForWorkersApi.class).updateRecordExportStatuses(statusRequest).execute();

        status = usersApi.getUploadStatus(session.getId()).execute().body();
        assertEquals(SynapseExporterStatus.NOT_EXPORTED, status.getRecord().getSynapseExporterStatus());
    }

    // returns the path relative to the root of the project
    private static String resolveFilePath(String fileLeafName) {
        String envName = user.getClientManager().getConfig().getEnvironment().name().toLowerCase(Locale.ENGLISH);
        return "src/test/resources/upload-test/" + envName + "/" + fileLeafName;
    }
}
