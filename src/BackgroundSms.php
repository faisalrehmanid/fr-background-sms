<?php

namespace FR\BackgroundSms;

use Exception;
use FR\BackgroundSms\Storage\StorageFactory;
use FR\BackgroundSms\Helper\Util;

/**
 * @author Faisal Rehman <faisalrehmanid@hotmail.com>
 * 
 * This class provide background mass sms implementation
 * 
 * How to use this class?
 * Check examples folder given on root
 * 
 */
class BackgroundSms
{
    /**
     * Configurations
     *
     * @var array
     */
    protected $config;

    /**
     * Configurations
     * 
     * NOTE: Same $config will be inject to the PHP script that will run specific worker in 
     * this case Execute.php will get this same $config
     *
     * @param array $config = [
     *  'logs_path' => '/var/www/virtual/test/httpdocs/public/faisalrehmanid-fr-background-sms/error-logs-gearman',
     *  'autoload_path' => '/var/www/virtual/test/httpdocs/public/faisalrehmanid-fr-background-sms/vendor/autoload.php',
     *  'execute_path' => '/var/www/virtual/test/httpdocs/public/faisalrehmanid-fr-background-sms/src/Execute.php',
     *  'job_details_url' => 'https:/testurl.com/job-details/{job_id}', // Optional
     *  'timezone' => 'Asia/Karachi',
     *  'number_of_retry_for_send_sms' => 10,
     *  'number_of_send_sms_workers_for_background_job' => 2,
     *  'gearman' => [
     *      'client' => [
     *          'servers' => '127.0.0.1:4730' // Multiple servers can be separate by a comma
     *      ],
     *      'worker' => [
     *          'servers' => '127.0.0.1:4730' // Multiple servers can be separate by a comma
     *      ]
     *  ], 
     *  'storage' => [
     *      'db' => [    // @see \FR\Db\DbFactory::init() for MySQL
     *          'driver' => 'pdo_mysql',
     *          'hostname' => 'localhost',
     *          'port' => '3306',
     *          'username' => 'root',
     *          'password' => '',
     *          'database' => 'test_fr_db_mysql',
     *          'charset' => 'utf8mb4'
     *      ],
     *   // 'db' => [    // @see \FR\Db\DbFactory::init() for Oracle
     *   //    'driver' => 'oci8',
     *   //    'connection' => 'ERPDEVDB',
     *   //    'username' => 'USERNAME',
     *   //    'password' => 'PASSWORD',
     *   //    'character_set' => 'AL32UTF8'
     *   // ],
     *      'vendors_table'   => 'test_fr_db_mysql.background_sms_vendors',
     *	    'jobs_table'      => 'test_fr_db_mysql.background_sms_jobs',
     *	    'sent_log_table'  => 'test_fr_db_mysql.background_sms_job_sent_log',
     *	    'templates_table' => 'test_fr_db_mysql.background_sms_job_templates'
     *  ],
     *  'oracle_home' => '/home/oracle/app/oracle/product/19.0.0/client_1', // Only for oracle
     *  'ld_library_path' => '/home/oracle/app/oracle/product/19.0.0/client_1/lib', // Only for oracle
     * 
     *  // Use `which` command to check complete path like `which php` and in this case /bin/php
     *  'cmd' => [
     *      'php' => '/bin/php',
     *      'pkill' => '/bin/pkill',
     *      'gearadmin' => '/bin/gearadmin'
     *  ],
     * 
     *  // List of response codes need to retry when esms 'Not Sent'. If empty array is given 
     *  // it will retry to send all 'Not Sent' sms but if codes are specified it will only 
     *  // consider to retry those `Not Sent` sms having that response code.
     *  'retry_exception_codes' => [
     *   432
     *  ]
     * ];
     */
    public function __construct(array $config)
    {
        // Where to log error and std output files
        @$logs_path      = $config['logs_path'];

        // File path of autoload.php
        @$autoload_path = $config['autoload_path'];

        // PHP script to run specific worker based on worker_id
        @$execute_path   = $config['execute_path'];

        // Timezone setting
        @$timezone  = $config['timezone'];
        // Set default timezone
        date_default_timezone_set($config['timezone']);

        // Number of retry when esms 'Not Sent'. Could be 0 if don't want to retry
        @$number_of_retry_for_send_sms = $config['number_of_retry_for_send_sms'];

        // Number of SendSmsWorker assign to each background job. Value must be atleast 1
        @$number_of_send_sms_workers_for_background_job = $config['number_of_send_sms_workers_for_background_job'];

        // Gearman config
        @$gearman = $config['gearman'];

        // Storage config where to store sent log
        @$storage = $config['storage'];

        // Optional: Used only when using Oracle storage
        @$oracle_home = $config['oracle_home'];

        // Optional: Used only when using Oracle storage
        @$ld_library_path = $config['ld_library_path'];

        // List of all cmd commands used in workers
        @$cmd = $config['cmd'];

        // List of response codes need to retry when esms 'Not Sent'
        @$retry_exception_codes = $config['retry_exception_codes'];

        // Validate logs_path
        if (
            !$logs_path ||
            !is_string($logs_path)
        )
            throw new \Exception('`logs_path` cannot be empty and must be string');
        // Logs path must be valid directory path
        if (!is_dir($logs_path)) {
            throw new \Exception('Directory not found at: `' . $logs_path . '`');
        }
        // Logs path must be writeable
        if (substr(sprintf('%o', fileperms($logs_path)), -4) != '0777')
            throw new \Exception('`' . $logs_path . '` dir permissions must be 777 for all files and directories recursively');

        // Validate autoload_path
        if (
            !$autoload_path ||
            !is_string($autoload_path)
        )
            throw new \Exception('`autoload_path` cannot be empty and must be string');
        //  autoload_path must be valid file path
        if (!is_file($autoload_path)) {
            throw new \Exception('File not found at: `' . $autoload_path . '`');
        }

        // Validate execute_path
        if (
            !$execute_path ||
            !is_string($execute_path)
        )
            throw new \Exception('`execute_path` cannot be empty and must be string');
        // Execute path must be valid PHP file
        if (!is_file($execute_path)) {
            throw new \Exception('File not found at: `' . $execute_path . '`');
        }

        // Validate timezone
        if (
            !$timezone ||
            !is_string($timezone)
        )
            throw new \Exception('`timezone` cannot be empty and must be string');

        // Validate number_of_retry_for_send_sms
        if (
            !is_int($number_of_retry_for_send_sms) ||
            $number_of_retry_for_send_sms < 0
        )
            throw new \Exception('`number_of_retry_for_send_sms` cannot be empty and must be positive int');

        // Validate number_of_send_sms_workers_for_background_job
        if (
            !is_int($number_of_send_sms_workers_for_background_job) ||
            $number_of_send_sms_workers_for_background_job < 1
        )
            throw new \Exception('`number_of_send_sms_workers_for_background_job` cannot be empty and must be int and value must be atleast 1');

        // Validate gearman
        if (
            empty($gearman) ||
            !is_array($gearman)
        )
            throw new \Exception('`gearman` cannot be empty and must be array');

        if (
            empty($gearman['client']) ||
            !is_array($gearman['client'])
        )
            throw new \Exception('`gearman` must have `client` key and must be array');

        if (
            empty($gearman['worker']) ||
            !is_array($gearman['worker'])
        )
            throw new \Exception('`gearman` must have `worker` key and must be array');

        if (
            !$gearman['client']['servers'] ||
            !is_string($gearman['client']['servers'])
        )
            throw new \Exception('`gearman[`client`]` must have key `servers` with string value');

        if (
            !$gearman['worker']['servers'] ||
            !is_string($gearman['worker']['servers'])
        )
            throw new \Exception('`gearman[`worker`]` must have key `servers` with string value');

        // Validate storage
        if (
            empty($storage) ||
            !is_array($storage)
        )
            throw new \Exception('`storage` cannot be empty and must be array');

        if (
            !$storage['db'] ||
            !is_array($storage['db'])
        )
            throw new \Exception('`storage` must have key `db` with array as value');

        if (
            !$storage['vendors_table'] ||
            !is_string($storage['vendors_table'])
        )
            throw new \Exception('`storage` must have key `vendors_table` with string value');

        $parts = explode('.', $storage['vendors_table']);
        if (count($parts) != 2)
            throw new \Exception('`vendors_table` name format must be like: schema.table_name');

        if (
            !$storage['jobs_table'] ||
            !is_string($storage['jobs_table'])
        )
            throw new \Exception('`storage` must have key `jobs_table` with string value');

        $parts = explode('.', $storage['jobs_table']);
        if (count($parts) != 2)
            throw new \Exception('`jobs_table` name format must be like: schema.table_name');

        if (
            !$storage['sent_log_table'] ||
            !is_string($storage['sent_log_table'])
        )
            throw new \Exception('`storage` must have key `sent_log_table` with string value');

        $parts = explode('.', $storage['sent_log_table']);
        if (count($parts) != 2)
            throw new \Exception('`sent_log_table` name format must be like: schema.table_name');

        if (
            !$storage['templates_table'] ||
            !is_string($storage['templates_table'])
        )
            throw new \Exception('`storage` must have key `templates_table` with string value');

        $parts = explode('.', $storage['templates_table']);
        if (count($parts) != 2)
            throw new \Exception('`templates_table` name format must be like: schema.table_name');

        // Validate oracle_home
        if (
            $oracle_home &&
            !is_string($oracle_home)
        )
            throw new \Exception('`oracle_home` must be string');

        // Validate ld_library_path
        if (
            $ld_library_path &&
            !is_string($ld_library_path)
        )
            throw new \Exception('`ld_library_path` must be string');

        // Validate cmd
        if (
            empty($cmd) ||
            !is_array($cmd)
        )
            throw new \Exception('`cmd` cannot be empty and must be array');
        if (
            !@$cmd['php'] ||
            !is_string(@$cmd['php'])
        )
            throw new \Exception('`php` command required in cmd key. Must be string');
        if (
            !@$cmd['pkill'] ||
            !is_string(@$cmd['pkill'])
        )
            throw new \Exception('`pkill` command required in cmd key. Must be string');
        if (
            !@$cmd['gearadmin'] ||
            !is_string(@$cmd['gearadmin'])
        )
            throw new \Exception('`gearadmin` command required in cmd key. Must be string');

        // Validate retry_exception_codes
        if (!is_array($retry_exception_codes))
            throw new \Exception('`retry_exception_codes` must be array');

        $this->config = $config;
    }

    /**
     * Get Storage
     *
     * @return object \FR\BackgroundSms\Storage\StorageInterface
     * @throws \Exception Databasae not connected
     */
    protected function getStorage()
    {
        $StorageFactory = new StorageFactory();
        $Storage = $StorageFactory->init($this->config['storage']);
        return $Storage;
    }

    /**
     * Create database structure if already not created
     *
     * @return bool return true when created otherwise false
     */
    public function createDBStructure()
    {
        return $this->getStorage()->createDBStructure();
    }

    /**
     * Send sms or mass sms in background
     *
     * @param array $recipients List of all recipients. Always will be 2D array [
     *      [
     *       // Vendor name required and its status must be Active in vendors database table
     *       "vendor_name" => "SMS4Connect",
     * 
     *       // SMS mask required
     *       "mask" => "Test Mask",
     * 
     *       // From JSON configurations required
     *       "from_json" => '{"id":"test","pass":"test-pass","mask":"Test Mask"}',
     *       
     *       // Message body required
     *       "body" => "SMS Message Body",
     * 
     *       // Recipient mobile number
     *       "to" => "92 3323169566",
     *      ],
     * ]
     * @param string $notify_to Optional and used to send nofication email 
     *      for background job status can be multiple and separated by ; 
     *      e.g, user1@test.com: User1; user2@test.com;
     *      Notification email will be sent:
     *           When job has been Started
     *           When job has been Completed
     *           When job has been Canceled
     * 
     * @return string $job_id Always return 64 char unique job id
     * @throws \Exception Database connection error
     *                    Database tables not found
     *                    Gearman error
     */
    public function send(array $recipients, $notify_to = '')
    {
        // Validate database connection and table structure
        $this->getStorage()->validateStorage();

        $config = $this->config;

        // Unique background worker id show in command line when run: `gearadmin --status`
        $worker_id = 'SmsBackgroundWorker-' . date('Y-m-d') . '-' . Util::generateUniqueId(16);
        // Start worker to listen for incomming jobs
        $command = $config['cmd']['php'] . " " . $config['execute_path'] . " " . $worker_id . " " . base64_encode(json_encode($config)) . " 1>" . $config['logs_path'] . "/SendMassSmsBackgroundOutput.log 2>" . $config['logs_path'] . "/SendMassSmsBackgroundError.log /dev/null & ";
        exec($command);
        // Background worker has been started but wait for 1 Sec to assgin background job to this worker
        // sleep(1); // Give some time to worker to be stable

        // Gearman client
        $client = new \GearmanClient();
        // Add server
        $client->addServers($config['gearman']['client']['servers']);

        // Generate unique 64 chars job_id
        $job_id = Util::generateUniqueId(64);

        $job = [
            'job_id'       => $job_id,
            'notify_to'    => $notify_to,
            'recipients'   => $recipients,
            'retry_number' => 0
        ];
        $job = json_encode($job);

        // Assign background job to that unique worker started above
        $client->doBackground($worker_id, $job);

        // When error return from gearman
        if ($client->returnCode() != GEARMAN_SUCCESS) {
            throw new \Exception('Gearman error');
        }

        return $job_id;
    }

    /**
     * Get sms balance for given vendor
     *
     * @param string $vendor_name
     * @param string $from_json
     * @return string Always return total number of available SMS
     */
    public function getSmsBalance($vendor_name, $from_json)
    {
        $vendor = $this->getStorage()->getVendorByName($vendor_name);
        if (empty($vendor))
            throw new \Exception('SMS vendor name `' . $vendor_name . '` not found');

        if (!$vendor['vender_get_balance_code'])
            throw new \Exception('Get balance code not found for vendor: ' . $vendor_name);

        @$from  = json_decode($from_json, true);

        eval($vendor['vender_get_balance_code']);

        return $balance;
    }

    /**
     * Get job details
     *
     * @param string $job_id
     * @return array
     */
    public function getJobById($job_id)
    {
        return $this->getStorage()->getJobById($job_id);
    }

    /**
     * Cancel background job using job_id
     *
     * @param string $job_id
     * @return void
     * @throws \Exception Invalid job id. job details not found
     * @throws \Exception Job status already completed
     * @throws \Exception Job status already canceled
     */
    public function cancelJob($job_id)
    {
        $config = $this->config;
        $job = $this->getStorage()->getJobById($job_id);

        if (empty($job))
            throw new \Exception('Invalid job id. Job details not found');

        if ($job['job_status'] == 'Completed')
            throw new \Exception('Job status already completed');

        if ($job['job_status'] == 'Canceled')
            throw new \Exception('Job status already canceled');

        // Shutdown running processes and drop gearman functions related to this job id
        Util::shutdownSmsBackgroundWorker(
            $config['cmd']['gearadmin'],
            $config['cmd']['pkill'],
            $job['sms_background_worker']
        );

        // Update job status to Canceled
        $job_canceled_count = ((int) $job['job_total_count'] - (int) $job['job_executed_count']);
        $this->getStorage()->updateCanceledStatus($job['job_id'], $job_canceled_count);

        $notify_to = $job['job_notify_to'];
        if (!empty($notify_to)) {
            // Get updated job details
            $job = $this->getStorage()->getJobById($job_id);

            // Prepare notification esms body part
            $vars = [];
            $vars['___JOB_DETAILS_URL___'] = str_replace('{job_id}', $job['job_id'], $config['job_details_url']);
            $vars['___JOB_ID___'] = $job['job_id'];
            $vars['___JOB_RETRY_NUMBER___'] = $job['job_retry_number'];
            $vars['___JOB_STATUS___'] = $job['job_status'];
            $vars['___JOB_TOTAL_COUNT___'] = $job['job_total_count'];
            $vars['___JOB_EXECUTED_COUNT___'] = $job['job_executed_count'];
            $vars['___JOB_SENT_COUNT___'] = $job['job_sent_count'];
            $vars['___JOB_NOT_SENT_COUNT___'] = $job['job_not_sent_count'];
            $vars['___JOB_CANCELED_COUNT___'] = $job['job_canceled_count'];
            $vars['___JOB_PERCENT_COMPLETED___'] = $job['job_percent_completed'];
            $vars['___JOB_TIME_SPENT___'] = $job['job_time_spent'];
            $vars['___JOB_STARTED_AT___']  = date('d M Y H:i:s', strtotime($job['job_started_at']));
            $vars['___JOB_CANCELED_AT___'] = date('d M Y H:i:s', strtotime($job['job_canceled_at']));
            $template = $this->getStorage()->getTemplateByCode('job_canceled_template');
            $body = $template['body'];
            foreach ($vars as $key => $value) {
                $body = str_replace($key, $value, $body);
            }
            $subject = $template['subject'];
            foreach ($vars as $key => $value) {
                $subject = str_replace($key, $value, $subject);
            }

            // Send notification email to notify_to
            Util::sendEmail(
                $template['from'],
                $template['smtp_json'],
                $subject,
                $body,
                $notify_to,
                $template['reply_to'],
                $template['cc'],
                $template['bcc']
            );
        }
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
        $this->getStorage()->deleteSentLog($upto);
    }
}
