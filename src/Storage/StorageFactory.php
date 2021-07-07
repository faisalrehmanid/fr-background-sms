<?php

namespace FR\BackgroundSms\Storage;

use FR\Db\DbFactory;

class StorageFactory
{
    /**
     * Initialize or create storage object based on driver selection
     * 
     * // PDO MySQL connection configuration
     * $config =  [
     *                  'db' => [
     *                                     'driver' => 'pdo_mysql',
     *		                               'hostname' => 'localhost',
     *                                     'port' => '3306',
     *                                     'username' => 'root',
     *                                     'password' => '',
     *                                     'database' => 'database-name',
     *                                     'charset' => 'utf8mb4'
     *                  ],
     *	                'vendors_table'   => 'background_sms_vendors',
     *	                'jobs_table'      => 'background_sms_jobs',
     *	                'sent_log_table'  => 'background_sms_job_sent_log',
     *	                'templates_table' => 'background_sms_job_templates'
     * ];
     *
     * // Oracle connection configuration
     * $config =  [
     *                  'db' => [
     *                                      'driver' => 'oci8',
     *		                                'connection' => 'ERPDEVDB',
     *		                                'username' => 'GAMES',
     *		                                'password' => 'GAMES',
     *		                                'character_set' => 'AL32UTF8'
     *                  ],
     *	                'vendors_table'   => 'background_sms_vendors',
     *	                'jobs_table'      => 'background_sms_jobs',
     *	                'sent_log_table'  => 'background_sms_job_sent_log',
     *	                'templates_table' => 'background_sms_job_templates'
     * ];
     *
     * @param array $config Storage configuration
     * @throws \Exception When invalid driver given in connection configuration
     * @return object \FR\BackgroundSms\Storage\StorageInterface
     */
    public function init(array $config)
    {
        $db = $config['db'];
        $driver = $db['driver'];

        $vendors_table = $config['vendors_table'];
        $jobs_table = $config['jobs_table'];
        $sent_log_table = $config['sent_log_table'];
        $templates_table = $config['templates_table'];

        $driver = strtolower($driver);
        if (in_array($driver, ['oci8'])) {

            $DB = new DbFactory();
            $DB = $DB->init($db);

            return new Oracle\Storage(
                $DB,
                $vendors_table,
                $jobs_table,
                $sent_log_table,
                $templates_table
            );
        }

        if (in_array($driver, ['pdo_mysql'])) {

            $DB = new DbFactory();
            $DB = $DB->init($db);

            return new MySQL\Storage(
                $DB,
                $vendors_table,
                $jobs_table,
                $sent_log_table,
                $templates_table
            );
        }

        $drivers = ['pdo_mysql', 'oci8'];
        throw new \Exception('Invalid driver. Driver must be: ' . implode(', ', $drivers));
    }
}
