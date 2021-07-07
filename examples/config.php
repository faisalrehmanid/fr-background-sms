<?php
require __DIR__ . '/../vendor/autoload.php';

$config = [
    'logs_path' => '/var/www/virtual/test/httpdocs/public/faisalrehmanid-fr-background-sms/error-logs-gearman',
    'autoload_path' => '/var/www/virtual/test/httpdocs/public/faisalrehmanid-fr-background-sms/vendor/autoload.php',
    'execute_path' => '/var/www/virtual/test/httpdocs/public/faisalrehmanid-fr-background-sms/src/Execute.php',
    'job_details_url' => 'https:/testurl.com/job-details/{job_id}', // Optional
    'timezone' => 'Asia/Karachi',
    'number_of_retry_for_send_sms' => 0,
    'number_of_send_sms_workers_for_background_job' => 10,
    'gearman' => [
        'client' => [
            'servers' => '127.0.0.1:4730' // Multiple servers can be separate by a comma
        ],
        'worker' => [
            'servers' => '127.0.0.1:4730' // Multiple servers can be separate by a comma
        ]
    ],
    'storage' => [
        'db' => [    // @see \FR\Db\DbFactory::init() for MySQL
            'driver' => 'pdo_mysql',
            'hostname' => 'localhost',
            'port' => '3306',
            'username' => 'root',
            'password' => '',
            'database' => 'test_fr_db_mysql',
            'charset' => 'utf8mb4',
        ],

        /*
        'db' => [    // @see \FR\Db\DbFactory::init() for Oracle
            'driver' => 'oci8',
            'connection' => 'ERPDEVDB',
            'username' => 'USER',
            'password' => 'PASSWORD',
            'character_set' => 'AL32UTF8',
        ],
        */

        'vendors_table'   => 'test_fr_db_mysql.background_sms_vendors',
        'jobs_table'      => 'test_fr_db_mysql.background_sms_jobs',
        'sent_log_table'  => 'test_fr_db_mysql.background_sms_job_sent_log',
        'templates_table' => 'test_fr_db_mysql.background_sms_job_templates'
    ],
    'oracle_home' => '/home/oracle/app/oracle/product/19.0.0/client_1', // Only for oracle
    'ld_library_path' => '/home/oracle/app/oracle/product/19.0.0/client_1/lib', // Only for oracle

    // Use `which` command to check complete path like `which php` and in this case /bin/php
    'cmd' => [
        'php' => '/bin/php',
        'pkill' => '/bin/pkill',
        'gearadmin' => '/bin/gearadmin'
    ],

    // List of response codes need to retry when sms 'Not Sent'. If empty array is given 
    // it will retry to send all 'Not Sent' sms but if codes are specified it will only 
    // consider to retry those `Not Sent` sms having that response code.
    'retry_exception_codes' => [
        432
    ]
];
