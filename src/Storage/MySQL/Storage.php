<?php

namespace FR\BackgroundSms\Storage\MySQL;

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
        $this->vendors_table = strtolower($vendors_table);
        $this->jobs_table = strtolower($jobs_table);
        $this->sent_log_table = strtolower($sent_log_table);
        $this->templates_table = strtolower($templates_table);
    }

    /**
     * Return SQL script of database structure
     *
     * @return string
     */
    public function getDBStructure()
    {
        $fk_id = uniqid();
        $script = " CREATE TABLE " . $this->vendors_table . " (
            `vendor_name` varchar(100) NOT NULL,
            `vendor_send_sms_code` text NOT NULL,
            `vender_get_balance_code` text,
            `vendor_status` enum('Active','Inactive') NOT NULL DEFAULT 'Active',
            PRIMARY KEY (`vendor_name`)
          );
        
        CREATE TABLE " . $this->jobs_table . " (
            `job_id` binary(32) NOT NULL,
            `job_status` enum('Started','Processing','Completed','Canceled') NOT NULL DEFAULT 'Started',
            `job_total_count` int(11) unsigned NOT NULL DEFAULT '0',
            `job_executed_count` int(11) unsigned NOT NULL DEFAULT '0',
            `job_sent_count` int(11) unsigned NOT NULL DEFAULT '0',
            `job_not_sent_count` int(11) unsigned NOT NULL DEFAULT '0',
            `job_canceled_count` int(11) unsigned NOT NULL DEFAULT '0',
            `job_percent_completed` varchar(5) NOT NULL,
            `job_time_spent` varchar(100) NOT NULL,
            `job_started_at` datetime NOT NULL,
            `job_ended_at` datetime DEFAULT NULL,
            `job_canceled_at` datetime DEFAULT NULL,
            `job_notify_to` varchar(1000) DEFAULT NULL,
            `job_retry_number` int(11) unsigned NOT NULL DEFAULT '0',
            `sms_background_worker` varchar(100) NOT NULL,
            PRIMARY KEY (`job_id`)
          );
          
          CREATE TABLE " . $this->sent_log_table . " (
            `job_id`             binary(32) NOT NULL,
            `retry_number`       int(11) unsigned NOT NULL,
            `vendor_name`        varchar(100)  DEFAULT NULL,
            `mask`               varchar(100)  DEFAULT NULL,
            `from_json`          varchar(1000) DEFAULT NULL,
            `body`               varchar(1000) DEFAULT NULL,
            `to`                 varchar(100)  DEFAULT NULL,
            `sent_at`            datetime      DEFAULT NULL, 
            `sent_status`        enum('Sent', 'Not Sent') DEFAULT 'Not Sent',
            `exception_code`     varchar(10)   DEFAULT NULL,
            `exception_message`  varchar(1000) DEFAULT NULL,
            `response_json`      text,
            KEY `fk_job_id_" . $fk_id . "` (`job_id`),
            CONSTRAINT `fk_job_id_" . $fk_id . "` FOREIGN KEY (`job_id`) REFERENCES " . $this->jobs_table . " (`job_id`) ON DELETE CASCADE
          );
          
          CREATE TABLE " . $this->templates_table . " (
            `template_code` varchar(50) NOT NULL,
            `template_description` varchar(100) NOT NULL,
            `smtp_json` varchar(1000) DEFAULT NULL,
            `from` varchar(1000) NOT NULL,
            `subject` varchar(1000) NOT NULL,
            `body` text NOT NULL,
            `reply_to` varchar(1000) DEFAULT NULL,
            `cc` varchar(1000) DEFAULT NULL,
            `bcc` varchar(1000) DEFAULT NULL,
            PRIMARY KEY (`template_code`)
          ); 
          
          /* Insert default email templates for notifications */
          INSERT  INTO "  . $this->templates_table . " (`template_code`,`template_description`,`smtp_json`,`from`,`subject`,`body`,`reply_to`,`cc`,`bcc`) 
                VALUES ('job_started_template','When background job started','smtp-json','from@test.com','Background Sms Job Started','<p>Dear Concern,</p>\r\n\r\n<p>Your background sms job has been Started. Click on the link below to see updated sent log:</p>\r\n\r\n<p> >> <a href=\"___JOB_DETAILS_URL___\">Click Here To View Job Details</a></p>\r\n\r\n<p>Job summary is given below:</p>\r\n\r\n<table>\r\n	<tr>\r\n		<td>Job ID</td>\r\n		<td>___JOB_ID___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Status</td>\r\n		<td>___JOB_STATUS___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Total Count</td>\r\n		<td>___JOB_TOTAL_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Executed Count</td>\r\n		<td>___JOB_EXECUTED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Sent Count</td>\r\n		<td>___JOB_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Not Sent Count</td>\r\n		<td>___JOB_NOT_SENT_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Percent Completed</td>\r\n		<td>___JOB_PERCENT_COMPLETED___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Time Spent</td>\r\n		<td>___JOB_TIME_SPENT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Started At</td>\r\n		<td>___JOB_STARTED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Ended At</td>\r\n		<td>___JOB_ENDED_AT___</td>\r\n	</tr>\r\n</table>\r\n\r\n<p>*** This is an automatically generated email, please do not reply ***</p>\r\n\r\n<p>Thanks!</p>','reply-to','cc','bcc'),
                       
                       ('job_completed_template','When background job completed','smtp-json','from@test.com','Background Sms Job Completed','<p>Dear Concern,</p>\r\n\r\n<p>Your background sms job has been Completed. Click on the link below to see updated sent log:</p>\r\n\r\n<p> >> <a href=\"___JOB_DETAILS_URL___\">Click Here To View Job Details</a></p>\r\n\r\n<p>Job summary is given below:</p>\r\n\r\n<table>\r\n	<tr>\r\n		<td>Job ID</td>\r\n		<td>___JOB_ID___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Status</td>\r\n		<td>___JOB_STATUS___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Total Count</td>\r\n		<td>___JOB_TOTAL_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Executed Count</td>\r\n		<td>___JOB_EXECUTED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Sent Count</td>\r\n		<td>___JOB_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Not Sent Count</td>\r\n		<td>___JOB_NOT_SENT_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Percent Completed</td>\r\n		<td>___JOB_PERCENT_COMPLETED___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Time Spent</td>\r\n		<td>___JOB_TIME_SPENT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Started At</td>\r\n		<td>___JOB_STARTED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Ended At</td>\r\n		<td>___JOB_ENDED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Number of Retry</td>\r\n		<td>___JOB_RETRY_NUMBER___</td>\r\n	</tr>\r\n</table>\r\n\r\n<p>*** This is an automatically generated email, please do not reply ***</p>\r\n\r\n<p>Thanks!</p>','reply-to','cc','bcc'),

                       ('job_canceled_template','When background job canceled','smtp-json','from@test.com','Background Sms Job Canceled','<p>Dear Concern,</p>\r\n\r\n<p>Your background sms job has been Canceled. Click on the link below to see updated sent log:</p>\r\n\r\n<p> >> <a href=\"___JOB_DETAILS_URL___\">Click Here To View Job Details</a></p>\r\n\r\n<p>Job summary is given below:</p>\r\n\r\n<table>\r\n	<tr>\r\n		<td>Job ID</td>\r\n		<td>___JOB_ID___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Status</td>\r\n		<td>___JOB_STATUS___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Total Count</td>\r\n		<td>___JOB_TOTAL_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Executed Count</td>\r\n		<td>___JOB_EXECUTED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Sent Count</td>\r\n		<td>___JOB_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Not Sent Count</td>\r\n		<td>___JOB_NOT_SENT_COUNT___</td>\r\n	</tr>\r\n<tr>\r\n		<td>Canceled Count</td>\r\n		<td>___JOB_CANCELED_COUNT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Percent Completed</td>\r\n		<td>___JOB_PERCENT_COMPLETED___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Time Spent</td>\r\n		<td>___JOB_TIME_SPENT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Started At</td>\r\n		<td>___JOB_STARTED_AT___</td>\r\n	</tr>\r\n	<tr>\r\n		<td>Canceled At</td>\r\n		<td>___JOB_CANCELED_AT___</td>\r\n	</tr>\r\n</table>\r\n\r\n<p>*** This is an automatically generated email, please do not reply ***</p>\r\n\r\n<p>Thanks!</p>','reply-to','cc','bcc');
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
        $query = ' SELECT table_schema, 
                          table_name 
                     FROM information_schema.tables 
                    WHERE LOWER(CONCAT(table_schema, \'.\' ,table_name)) 
                        IN (:vendors_table, :jobs_table, :sent_log_table, :templates_table) ';
        $values = [
            ':vendors_table' => str_replace('`', '', strtolower($this->vendors_table)),
            ':jobs_table' => str_replace('`', '', strtolower($this->jobs_table)),
            ':sent_log_table' => str_replace('`', '', strtolower($this->sent_log_table)),
            ':templates_table' => str_replace('`', '', strtolower($this->templates_table)),
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
        $query = ' SELECT table_schema, 
                          table_name 
                     FROM information_schema.tables 
                    WHERE LOWER(CONCAT(table_schema, \'.\' ,table_name)) 
                        IN (:vendors_table, :jobs_table, :sent_log_table, :templates_table) ';
        $values = [
            ':vendors_table' => str_replace('`', '', strtolower($this->vendors_table)),
            ':jobs_table' => str_replace('`', '', strtolower($this->jobs_table)),
            ':sent_log_table' => str_replace('`', '', strtolower($this->sent_log_table)),
            ':templates_table' => str_replace('`', '', strtolower($this->templates_table)),
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
                        ' . $exp->getUuid('job_id') . ' job_id,
                        job_status,
                        job_total_count,
                        job_executed_count,
                        job_sent_count,
                        job_not_sent_count,
                        job_canceled_count,
                        job_percent_completed,
                        job_time_spent,
                        ' . $exp->getDate("job_started_at") . ' job_started_at,
                        ' . $exp->getDate("job_ended_at") . ' job_ended_at,
                        ' . $exp->getDate("job_canceled_at") . ' job_canceled_at,
                        job_notify_to,
                        job_retry_number,
                        sms_background_worker
                    FROM '  . $this->jobs_table . '
                     WHERE ' . $exp->getUuid('job_id') . ' = :job_id
                     LIMIT 1 ';
        $values = [
            ':job_id' => $job_id
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
                        vendor_name,
                        vendor_send_sms_code,
                        vender_get_balance_code,
                        vendor_status
                    FROM '  . $this->vendors_table . '
                WHERE vendor_name   = :vendor_name  ';
        $values = [
            ':vendor_name'   => $vendor_name,
        ];
        $row = $this->DB->fetchRow($query, $values);

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

        $WHERE .= ' AND ' . $exp->getUuid('job_id') . ' = :job_id 
                    AND retry_number = :retry_number ';
        $values = array_merge($values, [
            ':job_id' => $job_id,
            ':retry_number' => (int) $retry_number
        ]);

        $IN = $exp->in($retry_exception_codes);
        if ($IN->getFragment()) {
            $WHERE .= ' AND exception_code ' . $IN->getFragment();
            $values = array_merge($values, $IN->getValues());
        }

        $query = ' SELECT ' . $exp->getUuid('job_id') . ' job_id,
                            `retry_number`,
                            `vendor_name`,
                            `mask`,
                            `from_json`,
                            `body`,
                            `to`,
                            ' . $exp->getDate('sent_at') . ' sent_at,
                            `sent_status`,
                            `exception_code`,
                            `exception_message`,
                            `response_json`
                FROM    '  . $this->sent_log_table . '
                WHERE   1 = 1 ' . $WHERE;

        $query .= " ORDER BY sent_at ASC ";
        $data = $this->DB->fetchRows($query, $values);

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
                        smtp_json,
                        `from`,
                        `subject`,
                        `body`,
                        reply_to,
                        cc,
                        bcc
                    FROM '  . $this->templates_table . '
                     WHERE LOWER(template_code) = LOWER(:template_code)
                     LIMIT 1 ';
        $values = [
            ':template_code' => $template_code
        ];
        $row = $this->DB->fetchRow($query, $values);
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
        $data['job_id']                 = $exp->setUuid($job_id);
        $data['job_status']             = substr(trim($job_status), 0, 20);
        $data['job_total_count']        = (int) trim($job_total_count);
        $data['job_executed_count']     = (int) trim($job_executed_count);
        $data['job_sent_count']         = (int) trim($job_sent_count);
        $data['job_not_sent_count']     = (int) trim($job_not_sent_count);
        $data['job_canceled_count']     = (int) trim($job_canceled_count);
        $data['job_percent_completed']  = substr(trim($job_percent_completed), 0, 5);
        $data['job_time_spent']         = substr(trim($job_time_spent), 0, 100);
        $data['job_started_at']         = $exp->setDate($job_started_at);
        $data['job_notify_to']          = substr(trim($job_notify_to), 0, 1000);
        $data['sms_background_worker']  = substr(trim($sms_background_worker), 0, 100);

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
        $data['job_id']             = $exp->setUuid($job_id);
        $data['retry_number']       = (int) trim($retry_number);
        $data['vendor_name']        = substr(trim($vendor_name), 0, 100);
        $data['mask']               = substr(trim($mask), 0, 100);
        $data['from_json']          = substr(trim($from_json), 0, 1000);
        $data['body']               = substr(trim($body), 0, 1000);
        $data['to']                 = substr(trim($to), 0, 100);
        $data['sent_at']            = $exp->setDate($sent_at);
        $data['sent_status']        = substr(trim($sent_status), 0, 10);
        $data['exception_code']     = substr(trim($exception_code), 0, 10);
        $data['exception_message']  = substr(trim($exception_message), 0, 1000);
        $data['response_json']      = $response_json;

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
                    job_executed_count = CASE 
                                WHEN job_total_count > job_executed_count 
                                    THEN (job_executed_count + 1) 
                                ELSE job_executed_count 
                            END,
					job_sent_count = CASE 
                                WHEN job_total_count > job_sent_count AND :sent_status = 'Sent'
                                    THEN (job_sent_count + 1) 
                                ELSE job_sent_count 
                            END,
					job_not_sent_count = CASE 
                                WHEN job_total_count > job_not_sent_count AND :sent_status = 'Not Sent'
                                    THEN (job_not_sent_count + 1) 
                                ELSE job_not_sent_count 
                            END,		
                    job_percent_completed = CASE 
                                WHEN job_total_count >= job_executed_count 
                                    THEN CONCAT(ROUND(((job_executed_count / job_total_count) * 100)),'%')
                                ELSE job_percent_completed
                            END,
                    job_status = CASE
                                WHEN job_total_count = job_executed_count THEN 'Completed'
                                WHEN job_executed_count > 0 THEN 'Processing'
                                ELSE 'Started'
                            END,
                    job_ended_at = CASE 
                                WHEN job_total_count = job_executed_count THEN STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')
                                ELSE job_ended_at
                            END,
                    job_time_spent = CONCAT(
                        IF((FLOOR(TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) / 86400)) > 1, CONCAT(FLOOR(TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) / 86400),  ' Days '), ''),
                        IF((FLOOR(TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) / 86400)) = 1, CONCAT(FLOOR(TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) / 86400),  ' Day '), ''),
                        
                        IF((FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 86400)/3600)) > 1, CONCAT(FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 86400)/3600),  ' Hours '), ''),
                        IF((FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 86400)/3600)) = 1, CONCAT(FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 86400)/3600),  ' Hour '), ''),
                        
                        IF((FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 3600)/60)) > 1, CONCAT(FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 3600)/60),  ' Minutes '), ''),
                        IF((FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 3600)/60)) = 1, CONCAT(FLOOR((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 3600)/60),  ' Minute '), ''),
                        
                        IF((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 60) > 1, CONCAT(TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 60,  ' Seconds '), ''),
                        IF((TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 60) = 1, CONCAT(TIMESTAMPDIFF(SECOND, job_started_at, STR_TO_DATE(:current_time, '%Y-%m-%d %H:%i:%s')) % 60,  ' Second '), '')
                    ), 
                    job_retry_number = :retry_number                   
                    WHERE " . $exp->getUuid("job_id") . " = :job_id ";
        $values = [
            ':job_id' => $job_id,
            ':retry_number' => $retry_number,
            ':sent_status' => $sent_status,
            ':current_time' => $sent_at
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
                    job_status = :job_status,
                    job_canceled_count = :job_canceled_count,
                    job_canceled_at  = " . $date->getFragment() .
            " WHERE 
                    " . $exp->getUuid("job_id") . " = :job_id ";
        $values = [
            ':job_status' => 'Canceled',
            ':job_canceled_count' => $job_canceled_count,
            ':job_id' => $job_id
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
                    WHERE   " . $exp->getDate("job_started_at") . " <= :upto ";
        $values = [
            ':upto' => $upto
        ];

        $this->DB->delete($query, $values);
    }
}
