<?php

namespace FR\BackgroundSms\Workers;

use FR\BackgroundSms\Storage\StorageInterface;
use FR\BackgroundSms\Helper\Util;

class SendSmsWorker
{
	/**
	 * Configurations
	 *
	 * @var array $config 
	 * @see \FR\BackgroundSms\BackgroundSms::__construct $config
	 */
	protected $config;

	/**
	 * Storage to store sent log
	 *
	 * @var object \FR\BackgroundSms\Storage\StorageInterface
	 */
	protected $Storage;

	/**
	 * Inject Storage and config to worker
	 *
	 * @param StorageInterface $Storage
	 * @param array $config
	 * @see \FR\BackgroundSms\BackgroundSms::__construct $config
	 */
	public function __construct(StorageInterface $Storage, array $config)
	{
		$this->Storage = $Storage;
		$this->config = $config;
	}

	/**
	 * Worker listening for incomming jobs
	 *
	 * @param string $worker_id Used to show in command line when run: `gearadmin --status`
	 * @return void
	 */
	public function listen($worker_id = 'SendSmsWorker')
	{
		// Gearman servers from config
		// Comma separated servers e.g. 127.0.0.1:4730,127.0.0.1:4731 
		$servers = $this->config['gearman']['worker']['servers'];

		$worker = new \GearmanWorker();
		$worker->addServers($servers);
		$worker->addFunction(
			$worker_id,
			[$this, "sendSms"],
			[
				'Storage' => $this->Storage,
			]
		);
		while ($worker->work()) {
			if ($worker->returnCode() != GEARMAN_SUCCESS) {
				throw new \Exception("Return code: " . $worker->returnCode());
				break;
			}
		}
	}

	/**
	 * Send single sms and save sent log in storage
	 *
	 * @param object | json $job
	 * @param array $context
	 * @return void
	 */
	public function sendSms(
		$job,
		&$context
	) {
		$Storage = $context['Storage'];

		/// sleep(10);
		if (is_object($job)) {
			$json = $job->workload();
		} else
			$json = $job;

		$data = json_decode($json, true);

		@$vendor_name = $data['vendor_name'];
		@$mask  = $data['mask'];
		@$from  = json_decode($data['from_json'], true);
		@$body  = $data['body'];
		@$to    = Util::filterMobileNumber($data['to']);
		$vendor = $this->Storage->getVendorByName($vendor_name);
		if (empty($vendor))
			throw new \Exception('SMS vendor name `' . $vendor_name . '` not found');

		if ($vendor['vendor_status'] != 'Active')
			throw new \Exception('SMS vendor name `' . $vendor_name . '` is not Active');

		if (!$vendor['vendor_send_sms_code'])
			throw new \Exception('Send sms code not found for vendor: ' . $vendor_name);

		eval($vendor['vendor_send_sms_code']);

		// Default to is same as what user has input
		$to_log = $data['to'];

		// If filtered $to is good and start with 923 than add space
		if ($to && substr($to, 0, 3) == '923')
			$to_log = '92 3' . substr($to, 3, strlen($to));

		// Insert log into database
		$sent_at = date('Y-m-d H:i:s');
		@$Storage->insertSentLog(
			$data['job_id'],
			$data['retry_number'],
			$data['vendor_name'],
			$data['mask'],
			$data['from_json'],
			$data['body'],
			$to_log,
			$sent_at,
			$sent_status,
			$exception_code,
			$exception_message,
			$response_json
		);

		// Update job stats details by job_id
		$Storage->updateJobStatsById(
			$data['job_id'],
			$data['retry_number'],
			$sent_status,
			$sent_at
		);
	}
}
