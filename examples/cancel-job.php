<?php
require __DIR__ . '/config.php';

use FR\BackgroundSms\BackgroundSms;

try {
    $BackgroundSms = new BackgroundSms($config);

    // Cancel background job using job_id
    $job_id = $_GET['job_id'];
    $BackgroundSms->cancelJob($job_id);

    echo 'Job has been canceled';
} catch (\Exception $e) {
    $message = $e->getMessage();
    echo $message;

    pr($e);
}
