<?php

namespace FR\BackgroundSms\Storage\Oracle;

use FR\Db\DbInterface;
use FR\BackgroundSms\Storage\StorageInterface;

class Storage implements StorageInterface
{
    /**
     * @var object FR\Db\DbInterface
     */
    protected $DB;

    /**
     * Vendors table Like: schema.table_name
     *
     * @var string
     */
    protected $vendors_table;

    /**
     * Jobs table name Like: schema.table_name
     *
     * @var string
     */
    protected $jobs_table;

    /**
     * Sent log table name Like: schema.table_name
     *
     * @var string
     */
    protected $sent_log_table;

    /**
     * Templates table name Like: schema.table_name
     *
     * @var string
     */
    protected $templates_table;

    /**
     * @param object FR\Db\DbInterface $DB
     * @param string $vendors_table Like: schema.table_name
     * @param string $jobs_table Like: schema.table_name
     * @param string $sent_log_table Like: schema.table_name
     * @param string $templates_table Like: schema.table_name
     * @throws \Exception `vendors_table` cannot be empty and must be string
     * @throws \Exception `jobs_table` cannot be empty and must be string
     * @throws \Exception `sent_log_table` cannot be empty and must be string
     * @throws \Exception `templates_table` cannot be empty and must be string
     */
    public function __construct(
        DBInterface $DB,
        $vendors_table,
        $jobs_table,
        $sent_log_table,
        $templates_table
    ) {
        if (
            !$vendors_table ||
            !is_string($vendors_table)
        )
            throw new \Exception('`vendors_table` cannot be empty and must be string');

        $parts = explode('.', $vendors_table);
        if (count($parts) != 2)
            throw new \Exception('`vendors_table` name format must be like: schema.table_name');

        if (
            !$jobs_table ||
            !is_string($jobs_table)
        )
            throw new \Exception('`jobs_table` cannot be empty and must be string');

        $parts = explode('.', $jobs_table);
        if (count($parts) != 2)
            throw new \Exception('`jobs_table` name format must be like: schema.table_name');

        if (
            !$sent_log_table ||
            !is_string($sent_log_table)
        )
            throw new \Exception('`sent_log_table` cannot be empty and must be string');

        $parts = explode('.', $sent_log_table);
        if (count($parts) != 2)
            throw new \Exception('`sent_log_table` name format must be like: schema.table_name');

        if (
            !$templates_table ||
            !is_string($templates_table)
        )
            throw new \Exception('`templates_table` cannot be empty and must be string');

        $parts = explode('.', $templates_table);
        if (count($parts) != 2)
            throw new \Exception('`templates_table` name format must be like: schema.table_name');

        $this->DB = $DB;
        $this->vendors_table = strtoupper($vendors_table);
        $this->jobs_table = strtoupper($jobs_table);
        $this->sent_log_table = strtoupper($sent_log_table);
        $this->templates_table = strtoupper($templates_table);
    }

    /**
     * Return SQL script of database structure
     *
     * @return string
     */
    public function getDBStructure()
    {
        $parts   = explode('.', $this->vendors_table);
        @$vendors_table_name  = $parts[1];

        $parts   = explode('.', $this->jobs_table);
        @$jobs_table_name  = $parts[1];

        $parts   = explode('.', $this->sent_log_table);
        @$sent_log_table_name  = $parts[1];

        $parts   = explode('.', $this->templates_table);
        @$templates_table_name  = $parts[1];

        $script = ' CREATE TABLE ' . $this->vendors_table . '
        (
            VENDOR_NAME              VARCHAR2(100 CHAR)   NOT NULL,
            VENDOR_SEND_SMS_CODE     CLOB                 NOT NULL,
            VENDER_GET_BALANCE_CODE  CLOB,
            VENDOR_STATUS            VARCHAR2(50 CHAR)    NOT NULL
        ); 
        
        CREATE UNIQUE INDEX '  . substr($this->vendors_table, 0, 27) . '_PK ON '  . $this->vendors_table . ' (VENDOR_NAME);
        ALTER TABLE '  . $this->vendors_table . ' ADD (
          CONSTRAINT ' . substr($vendors_table_name, 0, 27) . '_PK
          PRIMARY KEY (VENDOR_NAME) USING INDEX '  . substr($this->vendors_table, 0, 27) . '_PK
          ENABLE VALIDATE);
        
        CREATE TABLE '  . $this->jobs_table . '
        (
            JOB_ID                      RAW(32)                NOT NULL,
            JOB_STATUS                  VARCHAR2(20 CHAR)      NOT NULL,
            JOB_TOTAL_COUNT             NUMBER                 NOT NULL,
            JOB_EXECUTED_COUNT          NUMBER                 NOT NULL,
            JOB_SENT_COUNT              NUMBER                 NOT NULL,
            JOB_NOT_SENT_COUNT          NUMBER                 NOT NULL,
            JOB_CANCELED_COUNT          NUMBER                 NOT NULL,
            JOB_PERCENT_COMPLETED       VARCHAR2(5 CHAR)       NOT NULL,
            JOB_TIME_SPENT              VARCHAR2(100 CHAR)         NULL,
            JOB_STARTED_AT              DATE                   NOT NULL,
            JOB_ENDED_AT                DATE,
            JOB_CANCELED_AT             DATE,
            JOB_NOTIFY_TO               VARCHAR2(4000 CHAR),
            JOB_RETRY_NUMBER            NUMBER                 DEFAULT 0,
            SMS_BACKGROUND_WORKER       VARCHAR2(100 CHAR)      NOT NULL
        );
        
        CREATE UNIQUE INDEX '  . substr($this->jobs_table, 0, 27) . '_PK ON '  . $this->jobs_table . ' (JOB_ID);
        ALTER TABLE '  . $this->jobs_table . ' ADD (
          CONSTRAINT ' . substr($jobs_table_name, 0, 27) . '_PK
          PRIMARY KEY (JOB_ID) USING INDEX '  . substr($this->jobs_table, 0, 27) . '_PK
          ENABLE VALIDATE);

        CREATE TABLE '  . $this->sent_log_table . '
        (
            JOB_ID             RAW(32)           NOT NULL,
            RETRY_NUMBER       NUMBER,
            VENDOR_NAME        VARCHAR2(100 CHAR), 
            MASK               VARCHAR2(100 CHAR),
            FROM_JSON          VARCHAR2(4000 CHAR),
            "BODY"             VARCHAR2(4000 CHAR), 
            "TO"               VARCHAR2(100 CHAR),
            SENT_AT            DATE,
            SENT_STATUS        VARCHAR2(10 CHAR),
            EXCEPTION_CODE     VARCHAR2(10 CHAR),
            EXCEPTION_MESSAGE  VARCHAR2(4000 CHAR), 
            RESPONSE_JSON      CLOB
        );

        ALTER TABLE '  . $this->sent_log_table . ' ADD (
        CONSTRAINT ' . substr($sent_log_table_name, 0, 27) . '_FK 
        FOREIGN KEY (JOB_ID) 
        REFERENCES '  . $this->jobs_table . ' (JOB_ID)
        ON DELETE CASCADE
        ENABLE VALIDATE);  

        CREATE TABLE '  . $this->templates_table . '
        (
            TEMPLATE_CODE         VARCHAR2(50 CHAR)       NOT NULL,
            TEMPLATE_DESCRIPTION  VARCHAR2(100 CHAR)      NOT NULL,
            SMTP_JSON             VARCHAR2(4000 CHAR),
            "FROM"                VARCHAR2(4000 CHAR)     NOT NULL,
            "SUBJECT"             VARCHAR2(4000 CHAR)     NOT NULL,
            "BODY"                CLOB                    NOT NULL,
            REPLY_TO              VARCHAR2(4000 CHAR),
            CC                    VARCHAR2(4000 CHAR),
            BCC                   VARCHAR2(4000 CHAR)
        );

        CREATE UNIQUE INDEX '  . substr($this->templates_table, 0, 27) . '_PK ON '  . $this->templates_table . ' (TEMPLATE_CODE);
        ALTER TABLE '  . $this->templates_table . ' ADD (
        CONSTRAINT ' . substr($templates_table_name, 0, 27) . '_PK
        PRIMARY KEY (TEMPLATE_CODE) USING INDEX '  . substr($this->templates_table, 0, 27) . '_PK
        ENABLE VALIDATE); ';

        $script .= " 
          -- Insert default email templates for notifications 

          INSERT  INTO "  . $this->templates_table . "(TEMPLATE_CODE, TEMPLATE_DESCRIPTION, SMTP_JSON, \"FROM\", \"SUBJECT\", \"BODY\", REPLY_TO, CC, BCC)
            VALUES ('job_started_template','When background job started','smtp-json','from@test.com','Background Sms Job Started','<p>Dear Concern,</p>\r\n\r\n<p>Your background sms job has been Started. Click on the link below to see updated sent log:</p>\r\n\r\n<p> >> <a href=\"___JOB_DETAILS_URL___\">Click Here To View Job Details</a></p>\r\n\r\n<p>Job summary is given below:</p>\r\n\r\n<table>\r\n	<tr>\r\n		<td>Job ID</td>\r\n		<td>___JOB_ID___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Status</td>\r\n		<td>___JOB_STATUS___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Total Count</td>\r\n		<td>___JOB_TOTAL_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Executed Count</td>\r\n		<td>___JOB_EXECUTED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Sent Count</td>\r\n		<td>___JOB_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Not Sent Count</td>\r\n		<td>___JOB_NOT_SENT_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Percent Completed</td>\r\n		<td>___JOB_PERCENT_COMPLETED___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Time Spent</td>\r\n		<td>___JOB_TIME_SPENT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Started At</td>\r\n		<td>___JOB_STARTED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Ended At</td>\r\n		<td>___JOB_ENDED_AT___</td>\r\n	</tr>\r\n</table>\r\n\r\n<p>*** This is an automatically generated email, please do not reply ***</p>\r\n\r\n<p>Thanks!</p>','reply-to','cc','bcc'); 
           
          INSERT  INTO "  . $this->templates_table . "(TEMPLATE_CODE, TEMPLATE_DESCRIPTION, SMTP_JSON, \"FROM\", \"SUBJECT\", \"BODY\", REPLY_TO, CC, BCC) 
            VALUES ('job_completed_template','When background job completed','smtp-json','from@test.com','Background Sms Job Completed','<p>Dear Concern,</p>\r\n\r\n<p>Your background sms job has been Completed. Click on the link below to see updated sent log:</p>\r\n\r\n<p> >> <a href=\"___JOB_DETAILS_URL___\">Click Here To View Job Details</a></p>\r\n\r\n<p>Job summary is given below:</p>\r\n\r\n<table>\r\n	<tr>\r\n		<td>Job ID</td>\r\n		<td>___JOB_ID___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Status</td>\r\n		<td>___JOB_STATUS___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Total Count</td>\r\n		<td>___JOB_TOTAL_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Executed Count</td>\r\n		<td>___JOB_EXECUTED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Sent Count</td>\r\n		<td>___JOB_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Not Sent Count</td>\r\n		<td>___JOB_NOT_SENT_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Percent Completed</td>\r\n		<td>___JOB_PERCENT_COMPLETED___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Time Spent</td>\r\n		<td>___JOB_TIME_SPENT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Started At</td>\r\n		<td>___JOB_STARTED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Ended At</td>\r\n		<td>___JOB_ENDED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Number of Retry</td>\r\n		<td>___JOB_RETRY_NUMBER___</td>\r\n	</tr>\r\n</table>\r\n\r\n<p>*** This is an automatically generated email, please do not reply ***</p>\r\n\r\n<p>Thanks!</p>','reply-to','cc','bcc');
        
          INSERT  INTO "  . $this->templates_table . "(TEMPLATE_CODE, TEMPLATE_DESCRIPTION, SMTP_JSON, \"FROM\", \"SUBJECT\", \"BODY\", REPLY_TO, CC, BCC)
            VALUES ('job_canceled_template','When background job canceled','smtp-json','from@test.com','Background Sms Job Canceled','<p>Dear Concern,</p>\r\n\r\n<p>Your background sms job has been Canceled. Click on the link below to see updated sent log:</p>\r\n\r\n<p> >> <a href=\"___JOB_DETAILS_URL___\">Click Here To View Job Details</a></p>\r\n\r\n<p>Job summary is given below:</p>\r\n\r\n<table>\r\n	<tr>\r\n		<td>Job ID</td>\r\n		<td>___JOB_ID___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Status</td>\r\n		<td>___JOB_STATUS___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Total Count</td>\r\n		<td>___JOB_TOTAL_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Executed Count</td>\r\n		<td>___JOB_EXECUTED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Sent Count</td>\r\n		<td>___JOB_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Not Sent Count</td>\r\n		<td>___JOB_NOT_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Canceled Count</td>\r\n		<td>___JOB_CANCELED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Percent Completed</td>\r\n		<td>___JOB_PERCENT_COMPLETED___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Time Spent</td>\r\n		<td>___JOB_TIME_SPENT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Started At</td>\r\n		<td>___JOB_STARTED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Canceled At</td>\r\n		<td>___JOB_CANCELED_AT___</td>\r\n	</tr>\r\n</table>\r\n\r\n<p>*** This is an automatically generated email, please do not reply ***</p>\r\n\r\n<p>Thanks!</p>','reply-to','cc','bcc'); 
          ";

        return $script;
    }

    /**
     * Create database structure if already not created
     *
     * @return bool return true when created otherwise false
     */
    public function createDBStructure()
    {
        $query = ' SELECT OWNER, 
                          TABLE_NAME 
                    FROM ALL_TABLES
                        WHERE UPPER(OWNER || \'.\' || TABLE_NAME)
                            IN (:VENDORS_TABLE, :JOBS_TABLE, :SENT_LOG_TABLE, :TEMPLATES_TABLE) ';
        $values = [
            ':VENDORS_TABLE' => str_replace('"', '', strtoupper($this->vendors_table)),
            ':JOBS_TABLE' => str_replace('"', '', strtoupper($this->jobs_table)),
            ':SENT_LOG_TABLE' => str_replace('"', '', strtoupper($this->sent_log_table)),
            ':TEMPLATES_TABLE' => str_replace('"', '', strtoupper($this->templates_table)),
        ];
        $tables = $this->DB->fetchColumn($query, $values);
        if (empty($tables)) {
            $query = $this->getDBStructure();
            $this->DB->importSQL($query);
            return true;
        }

        return false;
    }

    /**
     * Validate storage
     *
     * @return void
     * @throws \Exception Database connection error
     *                    Database tables not found or missing some tables
     */
    public function validateStorage()
    {
        $query = ' SELECT OWNER, 
                          TABLE_NAME 
                    FROM ALL_TABLES
                        WHERE UPPER(OWNER || \'.\' || TABLE_NAME)
                            IN (:VENDORS_TABLE, :JOBS_TABLE, :SENT_LOG_TABLE, :TEMPLATES_TABLE) ';
        $values = [
            ':VENDORS_TABLE' => str_replace('"', '', strtoupper($this->vendors_table)),
            ':JOBS_TABLE' => str_replace('"', '', strtoupper($this->jobs_table)),
            ':SENT_LOG_TABLE' => str_replace('"', '', strtoupper($this->sent_log_table)),
            ':TEMPLATES_TABLE' => str_replace('"', '', strtoupper($this->templates_table)),
        ];
        $tables = $this->DB->fetchColumn($query, $values);
        if (count($tables) != 4)
            throw new \Exception('Database tables not found or missing some tables');
    }

    /**
     * Get job details
     *
     * @param string $job_id
     * @return array
     */
    public function getJobById($job_id)
    {
        $job_id = strtolower($job_id);
        $exp = $this->DB->getExpression();

        $query = ' SELECT 
                        ' . $exp->getUuid('JOB_ID') . ' JOB_ID,
                        JOB_STATUS,
                        JOB_TOTAL_COUNT,
                        JOB_EXECUTED_COUNT,
                        JOB_SENT_COUNT,
                        JOB_NOT_SENT_COUNT,
                        JOB_CANCELED_COUNT,
                        JOB_PERCENT_COMPLETED,
                        JOB_TIME_SPENT,
                        ' . $exp->getDate("JOB_STARTED_AT") . ' JOB_STARTED_AT,
                        ' . $exp->getDate("JOB_ENDED_AT") . ' JOB_ENDED_AT,
                        ' . $exp->getDate("JOB_CANCELED_AT") . ' JOB_CANCELED_AT,
                        JOB_NOTIFY_TO,
                        JOB_RETRY_NUMBER,
                        SMS_BACKGROUND_WORKER
                    FROM '  . $this->jobs_table . '
                     WHERE ' . $exp->getUuid('JOB_ID') . ' = :JOB_ID ';
        $values = [
            ':JOB_ID' => $job_id
        ];
        $row = $this->DB->fetchRow($query, $values);
        return $row;
    }

    /**
     * Get vendor details by name
     *
     * @param string $vendor_name
     * @return array
     */
    public function getVendorByName($vendor_name)
    {
        $query = ' SELECT  
                        VENDOR_NAME,
                        VENDOR_SEND_SMS_CODE,
                        VENDER_GET_BALANCE_CODE,
                        VENDOR_STATUS
                    FROM '  . $this->vendors_table . '
                WHERE VENDOR_NAME   = :VENDOR_NAME  ';
        $values = [
            ':VENDOR_NAME'   => $vendor_name,
        ];
        $row = $this->DB->fetchRow($query, $values);

        if (!empty($row)) {
            if (is_object($row['vendor_send_sms_code']))
                $row['vendor_send_sms_code'] = $row['vendor_send_sms_code']->load();

            if (is_object($row['vender_get_balance_code']))
                $row['vender_get_balance_code'] = $row['vender_get_balance_code']->load();
        }

        return $row;
    }

    /**
     * Get not sent data for given job id and retry number
     *
     * @param string $job_id
     * @param string $retry_number
     * @param array $retry_exception_codes if given only consider list of exception codes for retry
     * @return array
     */
    public function getNotSentDataForJobIdAndRetryNumber($job_id, $retry_number, array $retry_exception_codes)
    {
        $job_id = strtolower($job_id);
        $exp = $this->DB->getExpression();
        $WHERE  = '';
        $values = [];

        $WHERE .= ' AND ' . $exp->getUuid('JOB_ID') . ' = :JOB_ID 
                    AND RETRY_NUMBER = :RETRY_NUMBER ';
        $values = array_merge($values, [
            ':JOB_ID' => $job_id,
            ':RETRY_NUMBER' => (int) $retry_number
        ]);

        $IN = $exp->in($retry_exception_codes);
        if ($IN->getFragment()) {
            $WHERE .= ' AND EXCEPTION_CODE ' . $IN->getFragment();
            $values = array_merge($values, $IN->getValues());
        }

        $query = ' SELECT ' . $exp->getUuid('JOB_ID') . ' JOB_ID,
                            RETRY_NUMBER,
                            VENDOR_NAME,
                            MASK,
                            FROM_JSON,
                            "BODY",
                            "TO",
                            ' . $exp->getDate('SENT_AT') . ' SENT_AT,
                            SENT_STATUS,
                            EXCEPTION_CODE,
                            EXCEPTION_MESSAGE,
                            RESPONSE_JSON
                FROM    '  . $this->sent_log_table . '
                WHERE   1 = 1 ' . $WHERE;

        $query .= " ORDER BY SENT_AT ASC ";
        $data = $this->DB->fetchRows($query, $values);

        if (!empty($data)) {
            foreach ($data as $k => $row) {
                if (is_object($row['response_json']))
                    $data[$k]['response_json'] = $row['response_json']->load();
            }
        }

        return ['data' => $data];
    }

    /**
     * Get template details by code
     *
     * @param string $template_code
     * @return array
     */
    public function getTemplateByCode($template_code)
    {
        $query = ' SELECT 
                        SMTP_JSON,
                        "FROM",
                        "SUBJECT",
                        "BODY",
                        REPLY_TO,
                        CC,
                        BCC
                    FROM '  . $this->templates_table . '
                     WHERE LOWER(TEMPLATE_CODE) = LOWER(:TEMPLATE_CODE) ';
        $values = [
            ':TEMPLATE_CODE' => $template_code
        ];
        $row = $this->DB->fetchRow($query, $values);

        if (!empty($row)) {
            if (is_object($row['body']))
                $row['body'] = $row['body']->load();
        }

        return $row;
    }

    /**
     * Insert job details
     *
     * @param string $job_id
     * @param string $job_status
     * @param int $job_total_count
     * @param int $job_executed_count
     * @param int $job_sent_count
     * @param int $job_not_sent_count
     * @param int $job_canceled_count
     * @param string $job_percent_completed
     * @param string $job_time_spent
     * @param string $job_started_at Format Y-m-d H:i:s
     * @param string $job_notify_to
     * @param string $sms_background_worker Gearman background worker id
     * @return void
     */
    public function insertJob(
        $job_id,
        $job_status,
        $job_total_count,
        $job_executed_count,
        $job_sent_count,
        $job_not_sent_count,
        $job_canceled_count,
        $job_percent_completed,
        $job_time_spent,
        $job_started_at,
        $job_notify_to,
        $sms_background_worker
    ) {
        $exp = $this->DB->getExpression();

        $data = [];
        $data['JOB_ID']                  = $exp->setUuid($job_id);
        $data['JOB_STATUS']              = substr(trim($job_status), 0, 20);
        $data['JOB_TOTAL_COUNT']         = (int) trim($job_total_count);
        $data['JOB_EXECUTED_COUNT']      = (int) trim($job_executed_count);
        $data['JOB_SENT_COUNT']          = (int) trim($job_sent_count);
        $data['JOB_NOT_SENT_COUNT']      = (int) trim($job_not_sent_count);
        $data['JOB_CANCELED_COUNT']      = (int) trim($job_canceled_count);
        $data['JOB_PERCENT_COMPLETED']   = substr(trim($job_percent_completed), 0, 5);
        $data['JOB_TIME_SPENT']          = substr(trim($job_time_spent), 0, 100);
        $data['JOB_STARTED_AT']          = $exp->setDate($job_started_at);
        $data['JOB_NOTIFY_TO']           = substr(trim($job_notify_to), 0, 4000);
        $data['SMS_BACKGROUND_WORKER']   = substr(trim($sms_background_worker), 0, 100);

        $this->DB->insert($this->jobs_table, $data);
    }

    /**
     * Insert sent log
     *
     * @param string $job_id
     * @param string $retry_number
     * @param string $vendor_name
     * @param string $mask
     * @param string $from_json
     * @param string $body
     * @param string $to
     * @param string $sent_at Format Y-m-d H:i:s
     * @param string $sent_status
     * @param string $exception_code
     * @param string $exception_message
     * @param string $response_json
     * @return void
     */
    public function insertSentLog(
        $job_id,
        $retry_number,
        $vendor_name,
        $mask,
        $from_json,
        $body,
        $to,
        $sent_at,
        $sent_status,
        $exception_code,
        $exception_message,
        $response_json
    ) {
        $exp = $this->DB->getExpression();

        $data = [];
        $data['JOB_ID']             = $exp->setUuid($job_id);
        $data['RETRY_NUMBER']       = (int) trim($retry_number);
        $data['VENDOR_NAME']        = substr(trim($vendor_name), 0, 100);
        $data['MASK']               = substr(trim($mask), 0, 100);
        $data['FROM_JSON']          = substr(trim($from_json), 0, 4000);
        $data['BODY']               = substr(trim($body), 0, 4000);
        $data['TO']                 = substr(trim($to), 0, 100);
        $data['SENT_AT']            = $exp->setDate($sent_at);
        $data['SENT_STATUS']        = substr(trim($sent_status), 0, 10);
        $data['EXCEPTION_CODE']     = substr(trim($exception_code), 0, 10);
        $data['EXCEPTION_MESSAGE']  = substr(trim($exception_message), 0, 4000);
        $data['RESPONSE_JSON']      = $response_json;

        $this->DB->insert($this->sent_log_table, $data);
    }

    /**
     * Update job stats details by job_id
     *
     * @param string $job_id
     * @param int $retry_number
     * @param string $sent_status 'Sent' | 'Not Sent'
     * @param string $sent_at Format Y-m-d H:i:s
     * @return void
     */
    public function updateJobStatsById($job_id, $retry_number, $sent_status, $sent_at)
    {
        $exp = $this->DB->getExpression();

        $query = " UPDATE "  . $this->jobs_table . " SET 
                            JOB_EXECUTED_COUNT = CASE 
                            WHEN JOB_TOTAL_COUNT > JOB_EXECUTED_COUNT 
                                THEN (JOB_EXECUTED_COUNT + 1) 
                            ELSE JOB_EXECUTED_COUNT 
                        END,
                    JOB_SENT_COUNT = CASE 
                            WHEN JOB_TOTAL_COUNT > JOB_SENT_COUNT AND :SENT_STATUS = 'Sent'
                                THEN (JOB_SENT_COUNT + 1) 
                            ELSE JOB_SENT_COUNT 
                        END,
                    JOB_NOT_SENT_COUNT = CASE 
                            WHEN JOB_TOTAL_COUNT > JOB_NOT_SENT_COUNT AND :SENT_STATUS = 'Not Sent'
                                THEN (JOB_NOT_SENT_COUNT + 1) 
                            ELSE JOB_NOT_SENT_COUNT 
                        END,		
                    JOB_PERCENT_COMPLETED = CASE 
                            WHEN JOB_TOTAL_COUNT >= (JOB_EXECUTED_COUNT + 1) 
                                THEN CONCAT(ROUND((((JOB_EXECUTED_COUNT + 1) / JOB_TOTAL_COUNT) * 100)),'%')
                            ELSE JOB_PERCENT_COMPLETED
                        END,
                    JOB_STATUS = CASE
                            WHEN JOB_TOTAL_COUNT <= (JOB_EXECUTED_COUNT + 1) THEN 'Completed'
                            WHEN (JOB_EXECUTED_COUNT + 1) > 0 THEN 'Processing'
                            ELSE 'Started'
                        END,
                    JOB_ENDED_AT = CASE 
                            WHEN JOB_TOTAL_COUNT = (JOB_EXECUTED_COUNT + 1) THEN TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS')
                            ELSE JOB_ENDED_AT
                        END,
                    JOB_TIME_SPENT = TRIM(BOTH FROM 
                                        CASE WHEN TRUNC(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - ADD_MONTHS( JOB_STARTED_AT, MONTHS_BETWEEN(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS'), JOB_STARTED_AT))) = 1 THEN 
                                            TRUNC(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - ADD_MONTHS( JOB_STARTED_AT, MONTHS_BETWEEN(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS'), JOB_STARTED_AT))) || ' Day ' 
                                            WHEN TRUNC(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - ADD_MONTHS( JOB_STARTED_AT, MONTHS_BETWEEN(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS'), JOB_STARTED_AT))) > 1 THEN 
                                            TRUNC(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - ADD_MONTHS( JOB_STARTED_AT, MONTHS_BETWEEN(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS'), JOB_STARTED_AT))) || ' Days '
                                        ELSE
                                            ''
                                        END || 
                                        CASE WHEN TRUNC(24 * MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)) = 1 THEN 
                                            TRUNC(24 * MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)) || ' Hour '
                                            WHEN TRUNC(24 * MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)) > 1 THEN 
                                            TRUNC(24 * MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)) || ' Hours ' 
                                        END ||
                                        CASE WHEN TRUNC( MOD (MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60 ) = 1 THEN 
                                            TRUNC( MOD (MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60 ) || ' Minute '
                                            WHEN TRUNC( MOD (MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60 ) > 1 THEN 
                                            TRUNC( MOD (MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60 ) || ' Minutes '
                                        END || 
                                        CASE WHEN ROUND(MOD(MOD(MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60,1)*60) <= 1 THEN
                                            ROUND(MOD(MOD(MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60,1)*60) || ' Second '
                                            WHEN ROUND(MOD(MOD(MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60,1)*60) > 1 THEN
                                            ROUND(MOD(MOD(MOD(TO_DATE(:CURRENT_TIME, 'YYYY-MM-DD HH24:MI:SS') - JOB_STARTED_AT,1)*24,1)*60,1)*60) || ' Seconds '
                                            END
                                    ),
                    JOB_RETRY_NUMBER = :RETRY_NUMBER
                    WHERE " . $exp->getUuid("JOB_ID") . " = :JOB_ID ";
        $values = [
            ':JOB_ID' => $job_id,
            ':RETRY_NUMBER' => $retry_number,
            ':SENT_STATUS'  => $sent_status,
            ':CURRENT_TIME' => $sent_at,
        ];

        $this->DB->update($query, $values);
    }

    /**
     * Update job status canceled by job_id
     *
     * @param string $job_id
     * @param string $job_canceled_count
     * @return void
     */
    public function updateCanceledStatus($job_id, $job_canceled_count)
    {
        $job_id = strtolower($job_id);
        $exp = $this->DB->getExpression();

        $date = $exp->setDate(date('Y-m-d H:i:s'));
        $query = " UPDATE "  . $this->jobs_table . " SET
                    JOB_STATUS = :JOB_STATUS,
                    JOB_CANCELED_COUNT = :JOB_CANCELED_COUNT,
                    JOB_CANCELED_AT  = " . $date->getFragment() .
            " WHERE 
                       " . $exp->getUuid("JOB_ID") . " = :JOB_ID ";
        $values = [
            ':JOB_STATUS' => 'Canceled',
            ':JOB_CANCELED_COUNT' => $job_canceled_count,
            ':JOB_ID' => $job_id
        ];
        $values = array_merge($values, $date->getValues());

        $this->DB->update($query, $values);
    }

    /**
     * Delete sent log upto given datetime inclusive
     *
     * @param string $upto Datetime format: Y-m-d H:i:s
     * @return void
     * @throws \Exception Invalid datetime format it must be: `Y-m-d H:i:s`
     */
    public function deleteSentLog($upto)
    {
        // Validate $upto datetime format: Y-m-d H:i:s
        $date = \DateTime::createFromFormat('Y-m-d H:i:s', $upto);
        if (($date && $date->format('Y-m-d H:i:s') == $upto) == false)
            throw new \Exception('Invalid datetime format it must be: `Y-m-d H:i:s`');

        $exp = $this->DB->getExpression();

        $query = " DELETE FROM "  . $this->jobs_table . "
                    WHERE   " . $exp->getDate("JOB_STARTED_AT") . " <= :JOB_STARTED_AT ";
        $values = [
            ':JOB_STARTED_AT' => $upto
        ];

        $this->DB->delete($query, $values);
    }
}
