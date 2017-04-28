package org.sagebionetworks.bridge.exporter.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.bouncycastle.cms.CMSException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.Entity;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import retrofit2.Response;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.Environment;
import org.sagebionetworks.bridge.data.Archive;
import org.sagebionetworks.bridge.data.JsonArchiveFile;
import org.sagebionetworks.bridge.data.StudyUploadEncryptor;
import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.exporter.integration.config.BridgeExporterTestSpringConfig;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.UploadSchemasApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.CmsPublicKey;
import org.sagebionetworks.bridge.rest.model.HealthDataRecord;
import org.sagebionetworks.bridge.rest.model.Message;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.SynapseExporterStatus;
import org.sagebionetworks.bridge.rest.model.UploadFieldDefinition;
import org.sagebionetworks.bridge.rest.model.UploadFieldType;
import org.sagebionetworks.bridge.rest.model.UploadSchema;
import org.sagebionetworks.bridge.rest.model.UploadSchemaType;
import org.sagebionetworks.bridge.rest.model.UploadSession;
import org.sagebionetworks.bridge.rest.model.UploadStatus;
import org.sagebionetworks.bridge.rest.model.UploadValidationStatus;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.SqsHelper;

@ContextConfiguration(classes = BridgeExporterTestSpringConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@Category(IntegrationSmokeTest.class)
public class ExportTest {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(ExportTest.class);
    private static final String ZIP_FILE = "./legacy-survey.zip";
    private static final String OUTPUT_FILE_NAME = "./legacy-survey-encrypted";
    private static final String CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET = "record.id.override.bucket";

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

    private static final String TEST_FILES_A_2 = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"Yes\"],\n" +
            "  \"startDate\":\"2015-04-02T03:26:55-07:00\",\n" +
            "  \"questionTypeName\":\"SingleChoice\",\n" +
            "  \"item\":\"AAA\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";

    private static final String TEST_FILES_B_2 = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"fencing\", \"running\", 3],\n" +
            "  \"startDate\":\"2015-04-02T03:26:55-07:00\",\n" +
            "  \"questionTypeName\":\"MultipleChoice\",\n" +
            "  \"item\":\"BBB\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";

    private static final String TEST_FILES_A_3 = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"Yes\"],\n" +
            "  \"startDate\":\"2015-04-02T03:26:55-07:00\",\n" +
            "  \"questionTypeName\":\"SingleChoice\",\n" +
            "  \"item\":\"AAA\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";

    private static final String TEST_FILES_B_3 = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"fencing\", \"running\", 3],\n" +
            "  \"startDate\":\"2015-04-02T03:26:59-07:00\",\n" +
            "  \"questionTypeName\":\"MultipleChoice\",\n" +
            "  \"item\":\"BBB\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";


    private static final String DATE_TIME_NOW_STR = "2017-04-26T03:26:59-07:00";
    private static final String CURRENT_TIME_ZONE= "-0700";
    private static final int UPLOAD_STATUS_DELAY_MILLISECONDS = 5000;
    private static final int EXPORT_SINGLE_SECONDS = 10;
    private static final int EXPORT_ALL_SECONDS = 20;
    private static final int SYNAPSE_SECONDS = 1;

    // Retry up to 6 times, so we don't spend more than 30 seconds per test.
    private static final int UPLOAD_STATUS_DELAY_RETRIES = 6;
    private static final int EXPORT_RETRIES = 6;
    private static final int SYNAPSE_RETRIES = 9;

    private static final String USER_NAME = "synapse.user";
    private static final String SYNAPSE_API_KEY_NAME = "synapse.api.key";
    private static final String TEST_USER_ID_NAME = "test.synapse.user.id";

    private static String synapseUser;
    private static String synapseApiKey;
    private static Long testUserId; // test user exists in synapse

    private static String exporterSqsUrl;
    private static DateTime dateTimeNow = DateTime.parse(DATE_TIME_NOW_STR);

    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser user;

    private Config bridgeConfig;

    private TestUserHelper.TestUser admin;
    private String studyId;
    private String uploadId;
    private SynapseClient synapseClient;
    private Project project;
    private Team team;

    private Environment env;
    private String recordIdOverrideBucket;
    private String s3FileName;

    private Table ddbExportTimeTable;
    private Table ddbSynapseMetaTables;
    private Table ddbSynapseTables;
    private Table ddbRecordTable;

    private SqsHelper sqsHelper;
    private S3Helper s3Helper;
    private AmazonS3Client s3Client;
    private DynamoScanHelper ddbScanHelper;

    @Autowired
    public final void setBridgeConfig(Config bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
    }

    @Autowired
    public final void setEnv(Environment env) {
        this.env = env;
    }

    @Autowired
    public final void setSqsHelper(SqsHelper sqsHelper) {
        this.sqsHelper = sqsHelper;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Autowired
    public final void setS3Client(AmazonS3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Autowired
    final void setDdbScanHelper(DynamoScanHelper ddbScanHelper) {
        this.ddbScanHelper = ddbScanHelper;
    }

    @Resource(name = "ddbExportTimeTable")
    final void setDdbExportTimeTable(Table ddbExportTimeTable) {
        this.ddbExportTimeTable = ddbExportTimeTable;
    }

    @Resource(name = "ddbSynapseMetaTables")
    final void setDdbSynapseMetaTables(Table ddbExportTimeTable) {
        this.ddbSynapseMetaTables = ddbExportTimeTable;
    }

    @Resource(name = "ddbSynapseTables")
    final void setDdbSynapseTables(Table ddbExportTimeTable) {
        this.ddbSynapseTables = ddbExportTimeTable;
    }

    @Resource(name = "ddbRecordTable")
    public final void setDdbRecordTable(Table ddbRecordTable) {
        this.ddbRecordTable = ddbRecordTable;
    }

    private void setupProperties() throws IOException {
        synapseUser = bridgeConfig.get(USER_NAME);
        synapseApiKey = bridgeConfig.get(SYNAPSE_API_KEY_NAME);
        testUserId = Long.parseLong(bridgeConfig.get(TEST_USER_ID_NAME));
        exporterSqsUrl = bridgeConfig.get("exporter.request.sqs.queue.url");
        recordIdOverrideBucket = bridgeConfig.get(CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
    }

    @Before
    public void before() throws Exception {
        setupProperties();

        admin = TestUserHelper.getSignedInAdmin();
        synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUserName(synapseUser);
        synapseClient.setApiKey(synapseApiKey);

        // create test study
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);

        studyId = Tests.randomIdentifier(ExportTest.class);
        Study study = Tests.getStudy(studyId, null);
        studiesApi.createStudy(study).execute().body();

        // then create test users in this study
        developer = TestUserHelper.createAndSignInUser(ExportTest.class, studyId,false, Role.DEVELOPER);
        user = TestUserHelper.createAndSignInUser(ExportTest.class, studyId,true);

        // make the user account sharing upload to enable export
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        StudyParticipant userParticipant = usersApi.getUsersParticipantRecord().execute().body();
        userParticipant.sharingScope(SharingScope.ALL_QUALIFIED_RESEARCHERS);
        usersApi.updateUsersParticipantRecord(userParticipant).execute().body();

        // create and encrypt test files
        createAndEncryptTestFiles(TEST_FILES_A, TEST_FILES_B);

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

        ddbSynapseMetaTables.putItem(new Item().withPrimaryKey("tableName", studyId + "-appVersion"));
        ddbSynapseMetaTables.putItem(new Item().withPrimaryKey("tableName", studyId + "-status"));
        ddbSynapseTables.putItem(new Item().withPrimaryKey("schemaKey", studyId + "-legacy-survey-v1"));

        uploadId = testUpload("./legacy-survey-encrypted");

        // then create test synapse project and team
        createSynapseProjectAndTeam();
    }

    @After
    public void after() throws Exception {
        ddbSynapseMetaTables.deleteItem("tableName", studyId + "-appVersion");
        ddbSynapseMetaTables.deleteItem("tableName", studyId + "-status");
        ddbSynapseTables.deleteItem("schemaKey", studyId + "-legacy-survey-v1");

        if (studyId != null) {
            admin.getClient(StudiesApi.class).deleteStudy(studyId, true).execute();
        }
        if (project != null) {
            synapseClient.deleteEntityById(project.getId());
        }
        if (team != null) {
            synapseClient.deleteTeam(team.getId());
        }

        ddbExportTimeTable.deleteItem("studyId", studyId);

        if (StringUtils.isNotBlank(s3FileName)) {
            s3Client.deleteObject(recordIdOverrideBucket, s3FileName);
        }

        admin.signOut();
    }

    @Test
    public void testInstant() throws Exception {
        DateTime dateTimeBeforeExport = DateTime.now();
        Long epochBeforeExport = dateTimeBeforeExport.minusMinutes(20).getMillis(); // use for later verification
        LOG.info("Time before request instant exporting: " + dateTimeBeforeExport.toString());

        // modify uploadedOn field in record table to a fake datetime 20 min ago to avoid wait 1 min
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(epochBeforeExport));

        // then execute instant exporting against this new study, done by researcher/developer
        ForDevelopersApi forDevelopersApi = developer.getClient(ForDevelopersApi.class);
        Response<Message> response = forDevelopersApi.requestInstantExport().execute();
        assertEquals(202, response.code());

        // verify
        Item lastExportDateTime;
        Long lastExportDateTimeEpoch = getLastExportDateTime(epochBeforeExport);

        assertNotNull(lastExportDateTimeEpoch);

        // the time recorded in export time table should be equal to the date time we submit to the export request
        assertTrue(lastExportDateTimeEpoch > epochBeforeExport);

        verifyExport(uploadId, false, false, null, 0);

        // then do another instant export, verify if it will export the second upload and not the first upload
        // modify last export date time to 15 min ago
        long newLastExportDateTime = DateTime.now().minusMinutes(15).getMillis();
        ddbExportTimeTable.updateItem("studyId", studyId, new AttributeUpdate("lastExportDateTime").put(newLastExportDateTime));

        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // then modify new upload to 10 min ago to avoid gray period
        UploadValidationStatus
                uploadStatus2 = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId2).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus2.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusMinutes(10).getMillis()));

        // finally do instant export
        Response<Message> response2 = forDevelopersApi.requestInstantExport().execute();
        assertEquals(202, response2.code());

        // verify
        Long lastExportDateTimeEpochSecond = getLastExportDateTime(lastExportDateTimeEpoch);

        assertNotNull(lastExportDateTimeEpochSecond);

        // the time recorded in export time table should be equal to the date time we submit to the export request
        assertTrue(lastExportDateTimeEpochSecond > epochBeforeExport);

        verifyExport(uploadId2, false, false, uploadId, 1);

        // also verify the first upload was not exported twice

        lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
        lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
        assertNotNull(lastExportDateTimeEpoch);

        // the time received exporting request should be after the time before thread sleep
        assertTrue(lastExportDateTimeEpoch > newLastExportDateTime);
    }

    @Test
    public void testDailyOnlyTestStudy() throws Exception {
        // modify uploadedOn field in record table to a fake datetime earlier than yesterday's midnight -- this upload should never be exported
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusDays(1).withTimeAtStartOfDay().minusMillis(1).getMillis()));

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // and modify the uploadedOn to 1 hour ago -- this will be exported once
        uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId2).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusHours(1).getMillis()));

        // assert only the second upload is exported
        assertExport(null, ExportType.DAILY, uploadId2, false, false, uploadId, 0);
        //verifyNotExport(uploadId, 0);

        // then modify last export date time to 40 min ago for later verification
        ddbExportTimeTable.updateItem("studyId", studyId, new AttributeUpdate("lastExportDateTime").put(DateTime.now().minusMinutes(40).getMillis()));

        // then create another upload and modify to 30 min ago -- this will be exported once
        createAndEncryptTestFiles(TEST_FILES_A_3, TEST_FILES_B_3);
        String uploadId3 = testUpload("./legacy-survey-encrypted"); // using the same file name

        uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId3).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusMinutes(30).getMillis()));

        // assert only the third upload is exported -- that means it uses last export date time instead of default time range
        assertExport(null, ExportType.DAILY, uploadId3, false, false, uploadId2, 1);
        //verifyNotExport(uploadId2, 1);
    }

    @Test
    public void testDailyAllStudies() throws Exception {
        assertExport(null, ExportType.DAILY, uploadId, true, false, null, 0);
    }

    @Test
    public void testHourlyOnlyTestStudy() throws Exception {
        // modify uploadedOn field in record table to a fake datetime earlier than 2 hour ago
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusHours(2).getMillis()));

        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId2).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusMinutes(40).getMillis()));

        assertExport(null, ExportType.HOURLY, uploadId2, false, false, uploadId, 0);
        //verifyNotExport(uploadId, 0);

        ddbExportTimeTable.updateItem("studyId", studyId, new AttributeUpdate("lastExportDateTime").put(DateTime.now().minusMinutes(30).getMillis()));

        // then create another upload and modify to 30 min ago -- this will be exported once
        createAndEncryptTestFiles(TEST_FILES_A_3, TEST_FILES_B_3);
        String uploadId3 = testUpload("./legacy-survey-encrypted"); // using the same file name

        uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId3).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusMinutes(20).getMillis()));

        // check again
        assertExport(null, ExportType.HOURLY, uploadId3, false, false, uploadId2, 1);
        //verifyNotExport(uploadId2, 1);
    }

    @Test
    public void testDailyIgnoreLastExportDateTime() throws Exception {
        // first modify uploadedOn value for test upload
        UploadValidationStatus uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();
        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusMinutes(20).getMillis()));

        // then create a fake last export date time with value greater than uploadedOn
        ddbExportTimeTable.putItem(new Item().withPrimaryKey("studyId", studyId)
                .withNumber("lastExportDateTime", DateTime.now().minusMinutes(10).getMillis()));

        // then upload with ignore last export date time
        // we should see it exports last upload again
        assertExport(null, ExportType.DAILY, uploadId, false, true, null, 0);
    }

    @Test
    public void testDailyIgnoreLastExportDateTimeWithStartDateTime() throws Exception {
        // first modify uploadedOn value earlier than default start date time
        DateTime testStartDateTime = DateTime.now().minusDays(2);
        UploadValidationStatus uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();
        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(testStartDateTime.plusMillis(1).getMillis()));

        // then create a fake last export date time with value greater than uploadedOn
        ddbExportTimeTable.putItem(new Item().withPrimaryKey("studyId", studyId)
                .withNumber("lastExportDateTime", DateTime.now().minusMinutes(10).getMillis()));

        // and create request with start date time
        ObjectNode exporterRequest = JSON_OBJECT_MAPPER.createObjectNode();
        exporterRequest.put("startDateTime", testStartDateTime.toString());
        exporterRequest.put("endDateTime", DateTime.now().toString());
        exporterRequest.put("tag", "ex integ test");
        ArrayNode studyWhitelistArray = JSON_OBJECT_MAPPER.createArrayNode();
        studyWhitelistArray.add(studyId);
        exporterRequest.set("studyWhitelist", studyWhitelistArray);
        exporterRequest.put("ignoreLastExportTime", true);

        // then upload with ignore last export date time and with start date time
        // we should see it exports the upload
        assertExport(exporterRequest, null, uploadId, false, true, null, 0);
    }

    @Test
    public void testHourlyIgnoreLastExportDateTime() throws Exception {
        UploadValidationStatus uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();
        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusMinutes(20).getMillis()));

        // then create a fake last export date time with value greater than uploadedOn
        ddbExportTimeTable.putItem(new Item().withPrimaryKey("studyId", studyId)
                .withNumber("lastExportDateTime", DateTime.now().minusMinutes(10).getMillis()));

        // then upload with ignore last export date time
        // we should see it exports last upload again
        assertExport(null, ExportType.HOURLY, uploadId, false, true, null, 0);
    }

    @Test
    @Ignore
    // should ignore this test currently waiting for signal-end-of-export feature to complete
    public void testEndDateTimeBeforeLastExportDateTime() throws Exception {
        DateTime dateTimeBeforeExport = DateTime.now();
        ObjectNode exporterRequest = JSON_OBJECT_MAPPER.createObjectNode();
        exporterRequest.put("exportType", ExportType.HOURLY.name());
        exporterRequest.put("endDateTime", dateTimeBeforeExport.toString());
        ArrayNode studyWhitelistArray = JSON_OBJECT_MAPPER.createArrayNode();
        studyWhitelistArray.add(studyId);
        exporterRequest.set("studyWhitelist", studyWhitelistArray);

        ddbExportTimeTable.putItem(new Item().withPrimaryKey("studyId", studyId)
                .withNumber("lastExportDateTime", dateTimeBeforeExport.getMillis()));

        // the second one will not export anything -- synapse table will remain the same
        // case of endDateTime is equal to the last export date time
        assertExport(exporterRequest, null, uploadId, false, false, null, 0);

        // then test case of endDateTime before lastExportDateTime -- there should be no change as well
        exporterRequest = JSON_OBJECT_MAPPER.createObjectNode();
        exporterRequest.put("exportType", ExportType.HOURLY.name());
        exporterRequest.put("endDateTime", dateTimeBeforeExport.minusHours(1).toString());
        studyWhitelistArray = JSON_OBJECT_MAPPER.createArrayNode();
        studyWhitelistArray.add(studyId);
        exporterRequest.set("studyWhitelist", studyWhitelistArray);

        sqsHelper.sendMessageAsJson(exporterSqsUrl, JSON_OBJECT_MAPPER.writeValueAsString(exporterRequest), 0);

        Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
        Long lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
        assertNotNull(lastExportDateTimeEpoch);

        assertEquals((long)lastExportDateTimeEpoch, dateTimeBeforeExport.getMillis());
    }

    @Test
    public void testS3Override() throws Exception {
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        // first upload a test override file into s3 bucket
        s3FileName = "integ-test-redrive-record-ids." + DateTime.now().withZone(DateTimeZone.UTC).toString();
        List<String> recordIds = ImmutableList.of(uploadStatus.getRecord().getId());
        s3Helper.writeLinesToS3(recordIdOverrideBucket, s3FileName, recordIds);

        DateTime dateTimeBeforeExport = DateTime.now();
        LOG.info("Time before request daily exporting: " + dateTimeBeforeExport.toString());

        // then upload another test file
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted");

        ObjectNode exporterRequest = JSON_OBJECT_MAPPER.createObjectNode();
        exporterRequest.put("endDateTime", dateTimeBeforeExport.toString());
        ArrayNode studyWhitelistArray = JSON_OBJECT_MAPPER.createArrayNode();
        studyWhitelistArray.add(studyId);
        exporterRequest.set("studyWhitelist", studyWhitelistArray);
        exporterRequest.put("recordIdS3Override", s3FileName);
        exporterRequest.put("ignoreLastExportTime", true);

        // assert only the first upload was exported
        assertExport(exporterRequest, null, uploadId, false, true, uploadId2, 0);
        //verifyNotExport(uploadId2, 0);
    }

    private Long getLastExportDateTime(Long epochBeforeExport) throws InterruptedException {
        Long lastExportDateTimeEpoch = null;
        Item lastExportDateTime;
        for (int i = 0; i < EXPORT_RETRIES; i++) {
            lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
            if (lastExportDateTime != null) {
                lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
                if (lastExportDateTimeEpoch != null) {
                    if (epochBeforeExport < lastExportDateTimeEpoch) break;
                }
            }

            LOG.info("Retry get last export date time times: " + i);
            TimeUnit.SECONDS.sleep(EXPORT_SINGLE_SECONDS);
        }

        return lastExportDateTimeEpoch;
    }

    private void assertExport(ObjectNode exporterRequestOrigin, ExportType exportType, String uploadId,
            boolean exportAll, boolean ignoreLastExportDateTime, String notExpectUploadId, int notExpectUploadSize) throws IOException, InterruptedException, SynapseException {
        // prevent accidentally export all in prod env
        if (isProduction(env)) {
            if (exportAll || exporterRequestOrigin.get("studyWhitelist") == null) {
                LOG.info("Trying to export all studies in production env!");
                return;
            }
        }

        DateTime dateTimeBeforeExport = DateTime.now();
        ObjectNode exporterRequest = JSON_OBJECT_MAPPER.createObjectNode();

        if (exporterRequestOrigin == null) {
            exporterRequest.put("endDateTime", dateTimeBeforeExport.toString());
            exporterRequest.put("exportType", exportType.name());
            exporterRequest.put("tag", "ex integ test");
            if (!exportAll) {
                ArrayNode studyWhitelistArray = JSON_OBJECT_MAPPER.createArrayNode();
                studyWhitelistArray.add(studyId);
                exporterRequest.set("studyWhitelist", studyWhitelistArray);
            }

            exporterRequest.put("ignoreLastExportTime", ignoreLastExportDateTime);
        } else {
            exporterRequest = exporterRequestOrigin;
        }

        LOG.info("Time before request exporting: " + dateTimeBeforeExport.toString());

        sqsHelper.sendMessageAsJson(exporterSqsUrl, exporterRequest, 0);

        // verification
        // first verify export time table since it is the last step in exporter -- if its done, other tasks should be done as well
        if (exporterRequest.get("recordIdS3Override") != null) {
            Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
            assertNull(lastExportDateTime);
        } else {
            if (!ignoreLastExportDateTime) {
                verifyExportTime(DateTime.parse(exporterRequest.get("endDateTime").textValue()).getMillis(), exportAll);
            }
        }
        verifyExport(uploadId, exportAll, ignoreLastExportDateTime, notExpectUploadId, notExpectUploadSize);
    }

    private void verifyExportTime(long endDateTimeEpoch, boolean exportAll)
            throws InterruptedException {
        if (!exportAll) {
            Item lastExportDateTime ;
            Long lastExportDateTimeEpoch = null;

            for (int i = 0; i < EXPORT_RETRIES; i++) {
                lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
                if (lastExportDateTime != null) {
                    lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
                    if (lastExportDateTimeEpoch != null) {
                        if (endDateTimeEpoch == lastExportDateTimeEpoch) break;
                    }
                }

                LOG.info("Retry get last export date time times: " + i);
                TimeUnit.SECONDS.sleep(EXPORT_SINGLE_SECONDS);
            }

            assertNotNull(lastExportDateTimeEpoch);

            // the time recorded in export time table should be equal to the date time we submit to the export request
            assertEquals(endDateTimeEpoch, (long)lastExportDateTimeEpoch);
        } else {
            Iterable<Item> scanOutcomes;
            Long lastExportDateTimeEpoch;
            boolean shouldFail = false;
            outerLoop:
            for (int i = 0; i < 10; i++) {
                scanOutcomes = ddbScanHelper.scan(ddbExportTimeTable);

                // verify if all studies' last export date time are modified
                for (Item item: scanOutcomes) {
                    lastExportDateTimeEpoch = item.getLong("lastExportDateTime");
                    if (endDateTimeEpoch != lastExportDateTimeEpoch) {
                        LOG.info("Re-try get last export date time for all studies: " + i);
                        TimeUnit.SECONDS.sleep(EXPORT_ALL_SECONDS);
                        shouldFail = true;
                        continue outerLoop;
                    }
                }
                shouldFail = false;
                break;
            }
            if (shouldFail) fail("Last export date time is not equal to given end date time.");
        }
    }

    private void verifyExport(String uploadId, boolean exportAll, boolean ignoreLastExportDateTime, String notExpectUploadId, int notExpectUploadSize) throws SynapseException, InterruptedException, IOException {
        ForConsentedUsersApi forConsentedUsersApi = user.getClient(ForConsentedUsersApi.class);
        UploadValidationStatus uploadStatus = forConsentedUsersApi.getUploadStatus(uploadId).execute().body();

        // only wait for ignoreLastExportDateTime since in this case we cannot verify export time table right now
        if (ignoreLastExportDateTime) {
            for (int i = 0; i < EXPORT_RETRIES; i++) {
                LOG.info("Retry get export status times: " + i);
                if (exportAll) {
                    TimeUnit.SECONDS.sleep(EXPORT_ALL_SECONDS);
                } else {
                    TimeUnit.SECONDS.sleep(EXPORT_SINGLE_SECONDS);
                }

                uploadStatus = forConsentedUsersApi.getUploadStatus(uploadId).execute().body();
                if (uploadStatus.getRecord().getSynapseExporterStatus() == SynapseExporterStatus.SUCCEEDED) {
                    break;
                }
            }
        }

        assertEquals(SynapseExporterStatus.SUCCEEDED, uploadStatus.getRecord().getSynapseExporterStatus());

        // then, verify it creates correct synapse table in synapse and correct fields in tables
        Item appVersionItem = ddbSynapseMetaTables.getItem("tableName", studyId + "-appVersion");
        Item schemaKeyItem = ddbSynapseTables.getItem("schemaKey", studyId + "-legacy-survey-v1");

        // query table for expect export
        String tableIdAppVersion = appVersionItem.getString("tableId");

        assertFalse(StringUtils.isBlank(tableIdAppVersion));

        Entity tableAppVersion = synapseClient.getEntityById(tableIdAppVersion);
        assertEquals(TableEntity.class.getName(), tableAppVersion.getEntityType());

        assertEquals(project.getId(), tableAppVersion.getParentId()); // parent id of a table is project id

        // query synapse table
        String jobIdTokenAppVersion = synapseClient.queryTableEntityBundleAsyncStart("select * from " + tableIdAppVersion
                , 0L, 100L, true, 0xF, tableIdAppVersion);

        QueryResultBundle queryResultAppVersion = null;

        for (int j = 0; j < SYNAPSE_RETRIES; j++) {
            try {
                LOG.info("Retry get synapse table query result times: " + j);
                queryResultAppVersion = synapseClient.queryTableEntityBundleAsyncGet(jobIdTokenAppVersion, tableIdAppVersion);
                break;
            } catch (SynapseResultNotReadyException e) {
                TimeUnit.SECONDS.sleep(SYNAPSE_SECONDS);
            }
        }

        if (queryResultAppVersion == null) {
            fail("Query app version table failed.");
        }

        List<Row> tableContentsAppVersion = queryResultAppVersion.getQueryResult().getQueryResults().getRows();

        String tableIdSurvey = schemaKeyItem.getString("tableId");

        assertFalse(StringUtils.isBlank(tableIdSurvey));

        Entity tableSurvey = synapseClient.getEntityById(tableIdSurvey);
        assertEquals(TableEntity.class.getName(), tableSurvey.getEntityType());

        assertEquals(project.getId(), tableSurvey.getParentId()); // parent id of a table is project id

        // query synapse table
        String jobIdTokenSurvey = synapseClient.queryTableEntityBundleAsyncStart("select * from " + tableIdSurvey
                , 0L, 100L, true, 0xF, tableIdSurvey);

        QueryResultBundle queryResultSurvey = null;

        for (int j = 0; j < SYNAPSE_RETRIES; j++) {
            try {
                LOG.info("Retry get synapse table query result times: " + j);
                queryResultSurvey = synapseClient.queryTableEntityBundleAsyncGet(jobIdTokenSurvey, tableIdSurvey);
                break;
            } catch (SynapseResultNotReadyException e) {
                TimeUnit.SECONDS.sleep(SYNAPSE_SECONDS);
            }
        }

        if (queryResultSurvey == null) {
            fail("Query app version table failed.");
        }

        List<Row> tableContentsSurvey = queryResultSurvey.getQueryResult().getQueryResults().getRows();

        Map<String, List<Row>> queryMapAppVersion = convertRowsToMap(tableContentsAppVersion, queryResultAppVersion.getQueryResult().getQueryResults().getHeaders());
        Map<String, List<Row>> queryMapSurvey = convertRowsToMap(tableContentsSurvey, queryResultSurvey.getQueryResult().getQueryResults().getHeaders());

        assertNotNull(queryMapAppVersion.get(uploadStatus.getRecord().getId()));
        assertNotNull(queryMapSurvey.get(uploadStatus.getRecord().getId()));
        assertEquals(1, queryMapAppVersion.get(uploadStatus.getRecord().getId()).size());
        assertEquals(1, queryMapSurvey.get(uploadStatus.getRecord().getId()).size());

        List<SelectColumn> tableHeaders = queryResultAppVersion.getQueryResult().getQueryResults().getHeaders();
        List<String> rowContent = queryMapAppVersion.get(uploadStatus.getRecord().getId()).get(0).getValues();

        Item uploadRecord = ddbRecordTable.getItem("id", uploadStatus.getRecord().getId());
        // verify each column
        for (int i = 0; i < tableHeaders.size(); i++) {
            String headerName = tableHeaders.get(i).getName();
            String columnValue = rowContent.get(i);

            if (headerName.equals("originalTable")) {
                assertEquals(studyId + "-legacy-survey-v1", columnValue);
            } else {
                commonColumnsVerification(headerName, columnValue, uploadStatus, uploadRecord);
            }
        }

        List<SelectColumn> surveyTableHeaders = queryResultSurvey.getQueryResult().getQueryResults().getHeaders();
        List<String> surveyRowContent = queryMapSurvey.get(uploadStatus.getRecord().getId()).get(0).getValues();

        // verify each column
        for (int i = 0; i < surveyTableHeaders.size(); i++) {
            String headerName = surveyTableHeaders.get(i).getName();
            String columnValue = surveyRowContent.get(i);

            switch (headerName) {
                case "AAA": {
                    assertEquals("Yes", columnValue);
                    break;
                }
                case "BBB.fencing": {
                    assertEquals("true", columnValue);
                    break;
                }
                case "BBB.football": {
                    assertEquals("false", columnValue);
                    break;
                }
                case "BBB.running": {
                    assertEquals("true", columnValue);
                    break;
                }
                case "BBB.swimming": {
                    assertEquals("false", columnValue);
                    break;
                }
                case "BBB.3": {
                    assertEquals("true", columnValue);
                    break;
                }
                default: commonColumnsVerification(headerName, columnValue, uploadStatus, uploadRecord);
            }
        }

        // verify not expect export
        if (notExpectUploadId != null) {
            UploadValidationStatus notExpectUploadStatus = forConsentedUsersApi.getUploadStatus(notExpectUploadId).execute().body();
            String notExpectRecordId = notExpectUploadStatus.getRecord().getId();
            if (notExpectUploadSize == 0) {
                assertNull(queryMapAppVersion.get(notExpectRecordId));
                assertNull(queryMapSurvey.get(notExpectRecordId));
            } else {
                assertEquals(notExpectUploadSize, queryMapAppVersion.get(notExpectRecordId).size());
                assertEquals(notExpectUploadSize, queryMapSurvey.get(notExpectRecordId).size());
            }
        }
    }

    private void commonColumnsVerification(String headerName, String columnValue, UploadValidationStatus uploadStatus, Item uploadRecord) {
        switch (headerName) {
            case "recordId": {
                assertEquals(uploadStatus.getRecord().getId(), columnValue);
                break;
            }
            case "appVersion": {
                assertEquals("version 1.0.0, build 1", columnValue);
                break;
            }
            case "phoneInfo": {
                assertEquals("Integration Tests", columnValue);
                break;
            }
            case "uploadDate": {
                assertEquals(uploadRecord.getString("uploadDate"), columnValue);
                break;
            }
            case "healthCode": {
                assertEquals(uploadRecord.getString("healthCode"), columnValue);
                break;
            }
            case "externalId": {
                assertTrue(StringUtils.isBlank(columnValue));
                break;
            }
            case "dataGroups": {
                assertTrue(StringUtils.isBlank(columnValue));
                break;
            }
            case "createdOn": {
                assertEquals(String.valueOf(dateTimeNow.getMillis()), columnValue);
                break;
            }
            case "createdOnTimeZone": {
                assertEquals(CURRENT_TIME_ZONE, columnValue);
                break;
            }
            case "userSharingScope": {
                assertEquals(uploadRecord.get("userSharingScope"), columnValue);
                break;
            }
            default:
                LOG.info("Un-recognized column(s) added to synapse.");
        }
    }

    private Map<String, List<Row>> convertRowsToMap(List<Row> rows, List<SelectColumn> tableHeaders) {
        Map<String, List<Row>> retMap = new HashMap<>();
        for (Row row: rows) {
            List<String> oneRowContent = row.getValues();
            for (int i = 0; i < tableHeaders.size(); i++) {
                String headerName = tableHeaders.get(i).getName();
                String columnValue = oneRowContent.get(i);
                if (headerName.equals("recordId")) {
                    if (retMap.get(columnValue) == null) {
                        List<Row> rowList = new ArrayList<>();
                        rowList.add(row);
                        retMap.put(columnValue, rowList);
                    } else {
                        retMap.get(columnValue).add(row);
                    }
                }
            }
        }
        return retMap;
    }

    private void createAndEncryptTestFiles(String testFilesA, String testFilesB)
            throws IOException, CertificateException, CMSException {
        JsonArchiveFile test_file_A = new JsonArchiveFile("AAA.json", dateTimeNow, testFilesA);
        JsonArchiveFile test_file_B = new JsonArchiveFile("BBB.json", dateTimeNow, testFilesB);

        Archive archive = Archive.Builder.forActivity("legacy-survey", 1)
                .withAppVersionName("version 1.0.0, build 1")
                .withPhoneInfo("Integration Tests")
                .addDataFile(test_file_A)
                .addDataFile(test_file_B).build();

        // zip
        FileOutputStream fos = new FileOutputStream(ZIP_FILE);
        archive.writeTo(fos);

        // encrypt it
        String inputFilePath = new File(ZIP_FILE).getCanonicalPath();
        String outputFilePath = new File(OUTPUT_FILE_NAME).getCanonicalPath();

        ForDevelopersApi forDevelopersApi =  developer.getClient(ForDevelopersApi.class);
        CmsPublicKey publicKey = forDevelopersApi.getStudyPublicCsmKey().execute().body();

        StudyUploadEncryptor.writeTo(publicKey.getPublicKey(), inputFilePath, outputFilePath);
    }

    private boolean isProduction(Environment env) {
        return env == Environment.PROD;
    }

    private void createSynapseProjectAndTeam() throws IOException, SynapseException {
        StudiesApi studiesApi = developer.getClient(StudiesApi.class);
        Study currentStudy = studiesApi.getUsersStudy().execute().body();
        currentStudy.setSynapseDataAccessTeamId(null);
        currentStudy.setSynapseProjectId(null);
        currentStudy.setDisableExport(false); // make this study exportable

        studiesApi.updateUsersStudy(currentStudy).execute().body();

        // execute
        studiesApi.createSynapseProjectTeam(ImmutableList.of(testUserId.toString())).execute().body();

        // save project and team id
        Study newStudy = studiesApi.getUsersStudy().execute().body();
        assertEquals(newStudy.getIdentifier(), currentStudy.getIdentifier());
        String projectId = newStudy.getSynapseProjectId();
        Long teamId = newStudy.getSynapseDataAccessTeamId();

        Entity project = synapseClient.getEntityById(projectId);
        assertNotNull(project);
        assertEquals(project.getEntityType(), "org.sagebionetworks.repo.model.Project");
        this.project = (Project) project;
        Team team = synapseClient.getTeam(teamId.toString());
        assertNotNull(team);
        this.team = team;
    }

    private static String testUpload(String filePath) throws Exception {
        // set up request
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

        return uploadId;
    }

    private enum ExportType {
        HOURLY,
        DAILY,
        INSTANT;
    }
}
