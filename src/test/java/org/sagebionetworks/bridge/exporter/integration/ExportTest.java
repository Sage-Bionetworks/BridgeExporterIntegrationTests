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
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.bouncycastle.cms.CMSException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.Entity;
import org.sagebionetworks.repo.model.Project;
import org.sagebionetworks.repo.model.Team;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.TableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import retrofit2.Response;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.data.Archive;
import org.sagebionetworks.bridge.data.JsonArchiveFile;
import org.sagebionetworks.bridge.data.StudyUploadEncryptor;
import org.sagebionetworks.bridge.dynamodb.DynamoScanHelper;
import org.sagebionetworks.bridge.exporter.integration.config.BridgeExporterTestSpringConfig;
import org.sagebionetworks.bridge.exporter.record.ExportType;
import org.sagebionetworks.bridge.exporter.request.BridgeExporterRequest;
import org.sagebionetworks.bridge.exporter.util.BridgeExporterUtil;
import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
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
    private static final Logger LOG = LoggerFactory.getLogger(ExportTest.class);
    private static final String zipFile = "./legacy-survey.zip";
    private static final String outputFileName = "./legacy-survey-encrypted";

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
            "  \"choiceAnswers\":[\"No\"],\n" +
            "  \"startDate\":\"2015-04-02T03:26:57-07:00\",\n" +
            "  \"questionTypeName\":\"SingleChoice\",\n" +
            "  \"item\":\"AAA\",\n" +
            "  \"endDate\":\"2015-04-02T03:26:59-07:00\"\n" +
            "}";

    private static final String TEST_FILES_B_2 = "{\n" +
            "  \"questionType\":0,\n" +
            "  \"choiceAnswers\":[\"fencing\", \"football\", 3],\n" +
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


    private static String EXPORTER_SQS_URL;
    private static DateTime DATE_TIME_NOW = DateTime.now().minusHours(1);

    private static TestUserHelper.TestUser worker;
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser researcher;
    private static TestUserHelper.TestUser user;

    private Config bridgeConfig;

    private TestUserHelper.TestUser admin;
    private String studyId;
    private String uploadId;
    private SynapseClient synapseClient;
    private Project project;
    private Team team;

    private String env;
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
    public final void setEnv(String env) {
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
        SYNAPSE_USER = bridgeConfig.get(USER_NAME);
        SYNAPSE_API_KEY = bridgeConfig.get(SYNAPSE_API_KEY_NAME);
        EXPORTER_SYNAPSE_USER_ID = bridgeConfig.get(EXPORTER_SYNAPSE_USER_ID_NAME);
        TEST_USER_ID = Long.parseLong(bridgeConfig.get(TEST_USER_ID_NAME));
        EXPORTER_SQS_URL = bridgeConfig.get("exporter.request.sqs.queue.url");
        recordIdOverrideBucket = bridgeConfig.get(BridgeExporterUtil.CONFIG_KEY_RECORD_ID_OVERRIDE_BUCKET);
    }

    @Before
    public void before() throws Exception {
        setupProperties();

        admin = TestUserHelper.getSignedInAdmin();
        synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUserName(SYNAPSE_USER);
        synapseClient.setApiKey(SYNAPSE_API_KEY);

        // create test study
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);

        studyId = Tests.randomIdentifier(ExportTest.class);
        Study study = Tests.getStudy(studyId, null);
        studiesApi.createStudy(study).execute().body();

        // then create test users in this study
        worker = TestUserHelper.createAndSignInUser(ExportTest.class, studyId,false, Role.WORKER);
        developer = TestUserHelper.createAndSignInUser(ExportTest.class, studyId,false, Role.DEVELOPER);
        researcher = TestUserHelper.createAndSignInUser(ExportTest.class, studyId,true, Role.RESEARCHER);
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
    public void testInstant() throws Exception {
        DateTime dateTimeBeforeExport = DateTime.now();
        Long epochBeforeExport = dateTimeBeforeExport.getMillis(); // use for later verification
        LOG.info("Time before request instant exporting: " + dateTimeBeforeExport.toString());

        TimeUnit.MINUTES.sleep(1); // wait for gray period

        // then execute instant exporting against this new study, done by researcher/developer
        ForDevelopersApi forDevelopersApi = developer.getClient(ForDevelopersApi.class);
        Response<Message> response = forDevelopersApi.requestInstantExport().execute();
        assertEquals(202, response.code());

        // sleep a while to wait for exporter
        TimeUnit.SECONDS.sleep(20);

        // verify
        verifyExport(uploadId, 0, false);

        Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
        Long lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
        assertNotNull(lastExportDateTimeEpoch);

        // the time received exporting request should be after the time before thread sleep
        assertTrue(lastExportDateTimeEpoch > epochBeforeExport);
    }

    @Test
    public void testDailyOnlyTestStudy() throws Exception {
        // modify uploadedOn field in record table to a fake datetime earlier than 24 hours ago
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusHours(1).minusDays(1).getMillis()));

        assertExport(null, ExportType.DAILY, uploadId, 0, false, false, true);

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // check again
        assertExport(null, ExportType.DAILY, uploadId2, 0, false, false, false);
    }

    @Test
    public void testDailyAllStudies() throws Exception {
        // do NOT run this test on prod env
        if (isProduction(env)) return;

        assertExport(null, ExportType.DAILY, uploadId, 0, true, false, false);

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // check again
        assertExport(null, ExportType.DAILY, uploadId2, 1, true, false, false);
    }

    @Test
    public void testHourlyOnlyTestStudy() throws Exception {
        // modify uploadedOn field in record table to a fake datetime earlier than 1 hour ago
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusHours(2).getMillis()));

        assertExport(null, ExportType.HOURLY, uploadId, 0, false, false, true);

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // check again
        assertExport(null, ExportType.HOURLY, uploadId2, 0, false, false, false);
    }

    @Test
    public void testHourlyAllStudies() throws Exception {
        assertExport(null, ExportType.HOURLY, uploadId, 0, true, false, false);

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // check again
        assertExport(null, ExportType.HOURLY, uploadId2, 1, true, false, false);
    }

    @Test
    public void testDailyIgnoreLastExportDateTime() throws Exception {
        // upload a test record to create last export date time field
        assertExport(null, ExportType.DAILY, uploadId, 0, false, false, false);

        // modify uploadedOn field in record table to a fake datetime earlier than 24 hours ago
        // so that the first upload will not be exported again by exporter
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusDays(1).minusHours(1).getMillis()));

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // then upload with ignore last export date time
        assertExport(null, ExportType.DAILY, uploadId2, 1, false, true, false);
    }

    @Test
    public void testHourlyIgnoreLastExportDateTime() throws Exception {
        assertExport(null, ExportType.HOURLY, uploadId, 0, false, false, false);

        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();

        ddbRecordTable.updateItem("id", uploadStatus.getRecord().getId(),
                new AttributeUpdate("uploadedOn").put(DateTime.now().minusHours(2).getMillis()));

        // then export another upload
        createAndEncryptTestFiles(TEST_FILES_A_2, TEST_FILES_B_2);
        String uploadId2 = testUpload("./legacy-survey-encrypted"); // using the same file name

        // check again
        assertExport(null, ExportType.HOURLY, uploadId2, 1, false, true, false);
    }

    @Test
    public void testEndDateTimeBeforeLastExportDateTime() throws Exception {
        DateTime dateTimeBeforeExport = DateTime.now();
        BridgeExporterRequest exporterRequest = new BridgeExporterRequest.Builder().withExportType(ExportType.HOURLY)
                .withEndDateTime(dateTimeBeforeExport).withStudyWhitelist(ImmutableSet.of(studyId)).build();

        // the first one should be normal
        assertExport(exporterRequest, null, uploadId, 0, false, false, false);

        // the second one will not export anything -- synapse table will remain the same
        assertExport(exporterRequest, null, uploadId, 0, false, false, false);

        // then test endDateTime before lastExportDateTime -- there should be no change as well
        exporterRequest = new BridgeExporterRequest.Builder().withExportType(ExportType.HOURLY)
                .withEndDateTime(dateTimeBeforeExport.minusHours(1)).withStudyWhitelist(ImmutableSet.of(studyId)).build();

        sqsHelper.sendMessageAsJson(EXPORTER_SQS_URL, exporterRequest, 5);

        TimeUnit.MINUTES.sleep(1);

        Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
        Long lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
        assertNotNull(lastExportDateTimeEpoch);

        assertTrue(lastExportDateTimeEpoch > exporterRequest.getEndDateTime().getMillis());
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

        BridgeExporterRequest exporterRequest = new BridgeExporterRequest.Builder().withEndDateTime(dateTimeBeforeExport)
                .withRecordIdS3Override(s3FileName).build();

        // assert only the first upload was exported
        assertExport(exporterRequest, null, uploadId, 0, false, true, false);
    }

    private void verifyExport(String uploadId, int offset, boolean noUpload) throws SynapseException, InterruptedException, IOException {
        UploadValidationStatus
                uploadStatus = user.getClient(ForConsentedUsersApi.class).getUploadStatus(uploadId).execute().body();
        if (!noUpload) {
            assertEquals(SynapseExporterStatus.SUCCEEDED, uploadStatus.getRecord().getSynapseExporterStatus());
        } else {
            assertNull(uploadStatus.getRecord().getSynapseExporterStatus());
        }

        // then, verify it creates correct synapse table in synapse and correct fields in tables
        Item appVersionItem = ddbSynapseMetaTables.getItem("tableName", studyId + "-appVersion");
        Item statusItem = ddbSynapseMetaTables.getItem("tableName", studyId + "-status");
        Item schemaKeyItem = ddbSynapseTables.getItem("schemaKey", studyId + "-legacy-survey-v1");

        String appVersionTableId = appVersionItem.getString("tableId");
        String statusTableId = statusItem.getString("tableId");
        String schemaKeyTableId = schemaKeyItem.getString("tableId");

        if (noUpload) {
            assertTrue(StringUtils.isBlank(appVersionTableId));
            assertTrue(StringUtils.isBlank(statusTableId));
            assertTrue(StringUtils.isBlank(schemaKeyTableId));

        } else {
            assertFalse(StringUtils.isBlank(appVersionTableId));
            assertFalse(StringUtils.isBlank(statusTableId));
            assertFalse(StringUtils.isBlank(schemaKeyTableId));

            Entity appVersionTable = synapseClient.getEntityById(appVersionTableId);
            assertEquals(TableEntity.class.getName(), appVersionTable.getEntityType());

            assertEquals(project.getId(), appVersionTable.getParentId()); // parent id of a table is project id

            // verify fields
            Item uploadRecord = ddbRecordTable.getItem("id", uploadStatus.getRecord().getId());

            // app version
            String jobIdTokenAppVersion = synapseClient.queryTableEntityBundleAsyncStart("select * from " + appVersionTableId, 0L, 100L, true, 0xF, appVersionTableId);

            // also need to wait for query synapse table
            TimeUnit.SECONDS.sleep(30);

            QueryResultBundle queryResultAppVersion = synapseClient.queryTableEntityBundleAsyncGet(jobIdTokenAppVersion, appVersionTableId);
            List<Row> tableContents = queryResultAppVersion.getQueryResult().getQueryResults().getRows();

            // verify the size of the table is equal to the number of uploads
            assertEquals(offset + 1, tableContents.size());

            String appVersionRowStr = "[" + uploadStatus.getRecord().getId() + ", version 1.0.0, build 1, Integration Tests, " + uploadRecord.getString("uploadDate")
                    + ", " + uploadRecord.getString("healthCode") + ", null, null, " + DATE_TIME_NOW.getMillis()
                    + ", -0700, ALL_QUALIFIED_RESEARCHERS, " + studyId + "-legacy-survey-v1" + "]";

            assertEquals(appVersionRowStr, tableContents.get(offset).getValues().toString());

            // study status
            String jobIdTokenStudyStatus = synapseClient.queryTableEntityBundleAsyncStart("select * from " + statusTableId, 0L, 100L, true, 0xF, statusTableId);

            // also need to wait for query synapse table
            TimeUnit.SECONDS.sleep(30);

            QueryResultBundle queryResultStudyStatus = synapseClient.queryTableEntityBundleAsyncGet(jobIdTokenStudyStatus, statusTableId);
            List<Row> studyStatusTableContents = queryResultStudyStatus.getQueryResult().getQueryResults().getRows();

            // verify the size of the table is equal to the number of uploads
            assertEquals(offset + 1, studyStatusTableContents.size());

            String studyStatusRowStr = "[" + uploadStatus.getRecord().getUploadDate().toString() + "]";

            assertEquals(studyStatusRowStr, studyStatusTableContents.get(offset).getValues().toString());

            // survey table
            String jobIdTokenSurvey = synapseClient.queryTableEntityBundleAsyncStart("select * from " + schemaKeyTableId, 0L, 100L, true, 0xF, schemaKeyTableId);

            // also need to wait for query synapse table
            TimeUnit.SECONDS.sleep(30);

            QueryResultBundle queryResultSurvey = synapseClient.queryTableEntityBundleAsyncGet(jobIdTokenSurvey, schemaKeyTableId);
            List<Row> surveyTableContents = queryResultSurvey.getQueryResult().getQueryResults().getRows();

            // verify the size of the table is equal to the number of uploads
            assertEquals(offset + 1, surveyTableContents.size());

            String surveyRowStr;
            if (uploadId.equals(this.uploadId)) {
                surveyRowStr = "[" + uploadStatus.getRecord().getId() + ", version 1.0.0, build 1, Integration Tests, " + uploadRecord.getString("uploadDate")
                        + ", " + uploadRecord.getString("healthCode") + ", null, null, " + DATE_TIME_NOW.getMillis()
                        + ", -0700, ALL_QUALIFIED_RESEARCHERS, Yes, true, false, true, false, true" + "]";
            } else {
                surveyRowStr = "[" + uploadStatus.getRecord().getId() + ", version 1.0.0, build 1, Integration Tests, " + uploadRecord.getString("uploadDate")
                        + ", " + uploadRecord.getString("healthCode") + ", null, null, " + DATE_TIME_NOW.getMillis()
                        + ", -0700, ALL_QUALIFIED_RESEARCHERS, No, true, true, false, false, true" + "]";
            }

            assertEquals(surveyRowStr, surveyTableContents.get(offset).getValues().toString());
        }
    }

    private void verifyExportTime(long endDateTimeEpoch, boolean ignoreLastExportDateTime, boolean exportAll) {
        if (!ignoreLastExportDateTime) {
            if (!exportAll) {
                Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
                long lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
                assertNotNull(lastExportDateTimeEpoch);

                // the time recorded in export time table should be equal to the date time we submit to the export request
                assertEquals(lastExportDateTimeEpoch, endDateTimeEpoch);
            } else {
                Iterable<Item> scanOutcomes = ddbScanHelper.scan(ddbExportTimeTable);

                // verify if all studies' last export date time are modified
                for (Item item: scanOutcomes) {
                    long lastExportDateTimeEpoch = item.getLong("lastExportDateTime");
                    assertEquals(lastExportDateTimeEpoch, endDateTimeEpoch);
                }
            }
        } else {
            if (!exportAll) {
                Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
                Long lastExportDateTimeEpoch = lastExportDateTime.getLong("lastExportDateTime");
                assertNotNull(lastExportDateTimeEpoch);

                assertTrue(lastExportDateTimeEpoch < endDateTimeEpoch);
            } else {
                Iterable<Item> scanOutcomes = ddbScanHelper.scan(ddbExportTimeTable);

                // verify if all studies' last export date time are modified
                for (Item item: scanOutcomes) {
                    Long lastExportDateTimeEpoch = item.getLong("lastExportDateTime");
                    assertTrue(lastExportDateTimeEpoch < endDateTimeEpoch);
                }
            }
        }
    }

    private void assertExport(BridgeExporterRequest exporterRequestOri, ExportType exportType, String uploadId, int offset, boolean exportAll, boolean ignoreLastExportDateTime, boolean noUpload) throws IOException, InterruptedException, SynapseException {
        DateTime dateTimeBeforeExport;
        BridgeExporterRequest exporterRequest;

        if (exporterRequestOri == null) {
            dateTimeBeforeExport = DateTime.now();
            BridgeExporterRequest.Builder exporterRequestBuilder;
            if (!exportAll) {
                exporterRequestBuilder = new BridgeExporterRequest.Builder().withEndDateTime(dateTimeBeforeExport).withExportType(
                        exportType).withStudyWhitelist(ImmutableSet.of(studyId)).withTag("ex integ test");
            } else {
                exporterRequestBuilder = new BridgeExporterRequest.Builder().withEndDateTime(dateTimeBeforeExport).withExportType(
                        exportType).withTag("ex integ test export all");
            }

            exporterRequestBuilder.withIgnoreLastExportTime(ignoreLastExportDateTime);

            exporterRequest = exporterRequestBuilder.build();
        } else {
            exporterRequest = exporterRequestOri;
        }

        LOG.info("Time before request daily exporting: " + exporterRequest.getEndDateTime().toString());

        sqsHelper.sendMessageAsJson(EXPORTER_SQS_URL, exporterRequest, 5);

        TimeUnit.MINUTES.sleep(1);

        // verification
        verifyExport(uploadId, offset, noUpload);
        if (StringUtils.isNotBlank(exporterRequest.getRecordIdS3Override())) {
            Item lastExportDateTime = ddbExportTimeTable.getItem("studyId", studyId);
            assertNull(lastExportDateTime);
        } else {
            verifyExportTime(exporterRequest.getEndDateTime().getMillis(), ignoreLastExportDateTime, exportAll);
        }
    }

    private void createAndEncryptTestFiles(String testFilesA, String testFilesB)
            throws IOException, CertificateException, CMSException {
        JsonArchiveFile test_file_A = new JsonArchiveFile("AAA.json", DATE_TIME_NOW, testFilesA);
        JsonArchiveFile test_file_B = new JsonArchiveFile("BBB.json", DATE_TIME_NOW, testFilesB);

        Archive archive = Archive.Builder.forActivity("legacy-survey", 1)
                .withAppVersionName("version 1.0.0, build 1")
                .withPhoneInfo("Integration Tests")
                .addDataFile(test_file_A)
                .addDataFile(test_file_B).build();

        // zip
        FileOutputStream fos = new FileOutputStream(zipFile);
        archive.writeTo(fos);

        // encrypt it
        String inputFilePath = new File(zipFile).getCanonicalPath();
        String outputFilePath = new File(outputFileName).getCanonicalPath();

        ForDevelopersApi forDevelopersApi =  developer.getClient(ForDevelopersApi.class);
        CmsPublicKey publicKey = forDevelopersApi.getStudyPublicCsmKey().execute().body();

        StudyUploadEncryptor.writeTo(publicKey.getPublicKey(), inputFilePath, outputFilePath);
    }

    private boolean isProduction(String env) {
        return env.equals("prod");
    }

    private void createSynapseProjectAndTeam() throws IOException, SynapseException {
        StudiesApi studiesApi = developer.getClient(StudiesApi.class);
        Study currentStudy = studiesApi.getUsersStudy().execute().body();
        currentStudy.setSynapseDataAccessTeamId(null);
        currentStudy.setSynapseProjectId(null);
        currentStudy.setDisableExport(false); // make this study exportable

        studiesApi.updateUsersStudy(currentStudy).execute().body();

        // execute
        studiesApi.createSynapseProjectTeam(ImmutableList.of(TEST_USER_ID.toString())).execute().body();

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

        return uploadId;
    }
}
