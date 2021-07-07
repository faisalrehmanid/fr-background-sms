<?php

namespace FR\BackgroundSms\Storage;

interface StorageInterface
{
    /**
     * Return SQL script of database structure
     *
     * @return string
     */
    public function getDBStructure();

    /**
     * Create database structure if already not created
     *
     * @return bool return true when created otherwise false
     */
    public function createDBStructure();

    /**
     * Validate storage
     *
     * @return void
     * @throws \Exception Database connection error
     *                    Database tables not found
     */
    public function validateStorage();

    /**
     * Get job details
     *
     * @param string $job_id
     * @return array
     */
    public function getJobById($job_id);

    /**
     * Get vendor details by name
     *
     * @param string $vendor_name
     * @return array
     */
    public function getVendorByName($vendor_name);

    /**
     * Get not sent data for given job id and retry number
     *
     * @param string $job_id
     * @param string $retry_number
     * @param array $retry_exception_codes if given only consider list of exception codes for retry
     * @return array
     */
    public function getNotSentDataForJobIdAndRetryNumber($job_id, $retry_number, array $retry_exception_codes);

    /**
     * Get template details by code
     *
     * @param string $template_code
     * @return array
     */
    public function getTemplateByCode($template_code);

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
    );

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
    );

    /**
     * Update job stats details by job_id
     *
     * @param string $job_id
     * @param int $retry_number
     * @param string $sent_status 'Sent' | 'Not Sent'
     * @param string $sent_at Format Y-m-d H:i:s
     * @return void
     */
    public function updateJobStatsById($job_id, $retry_number, $sent_status, $sent_at);

    /**
     * Update job status canceled by job_id
     *
     * @param string $job_id
     * @param string $job_canceled_count
     * @return void
     */
    public function updateCanceledStatus($job_id, $job_canceled_count);

    /**
     * Delete sent log upto given datetime inclusive
     *
     * @param string $upto Datetime format: Y-m-d H:i:s
     * @return void
     * @throws \Exception Invalid datetime format it must be: `Y-m-d H:i:s`
     */
    public function deleteSentLog($upto);
}
