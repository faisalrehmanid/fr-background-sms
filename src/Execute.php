<?php

/**
 * Variables passed from command line
 * 
 * @param string $worker_id Unique id for worker
 * @param json $config Configurations
 * @see \FR\BackgroundSms\BackgroundSms::__construct $config
 */
$worker_id = $argv[1];
$config    = json_decode(base64_decode($argv[2]), true);

// Autoload.php path
require $config['autoload_path'];

/**
 * This script will execute/run specific worker based on worker_id given from 
 * command line and listen for incomming jobs
 */

use FR\BackgroundSms\Storage\StorageFactory;
use FR\BackgroundSms\Workers\SendMassSmsBackgroundWorker;
use FR\BackgroundSms\Workers\SendSmsWorker;
use FR\BackgroundSms\Helper\Util;

// Enable error logs
$logs_path = $config['logs_path'];
$logs_file_path = $logs_path . '/ExecuteError-SMS.log';
// Delete log file if already exists
Util::deleteFile($logs_file_path);
error_reporting(E_ALL);
ini_set('error_log', $logs_file_path);
ini_set('log_errors', true);

// Global configurations
ini_set('memory_limit', '-1');
ini_set('max_execution_time', 0);
date_default_timezone_set($config['timezone']);

// Set env oracle home variable if given in config
if (@$config['oracle_home'])
	putenv('ORACLE_HOME=' . $config['oracle_home']);

// Set LD library path variable if given in config
if (@$config['ld_library_path'])
	putenv('LD_LIBRARY_PATH=' . $config['ld_library_path']);

/**
 * Create storage object
 */
$StorageFactory = new StorageFactory();
$Storage = $StorageFactory->init($config['storage']);

// Start listening worker based on worker_id
if (strpos($worker_id, 'SmsBackgroundWorker') !== false) {
	$SendMassSmsBackgroundWorker = new SendMassSmsBackgroundWorker($Storage, $config);
	$SendMassSmsBackgroundWorker->listen($worker_id);
} else if (strpos($worker_id, 'SendSmsWorker') !== false) {
	$SendSmsWorker = new SendSmsWorker($Storage, $config);
	$SendSmsWorker->listen($worker_id);
} else
	throw new \Exception('Invalid worker. Worker must be: SendSmsWorker, SmsBackgroundWorker');
