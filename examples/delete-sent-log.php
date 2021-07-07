<?php
require __DIR__ . '/config.php';

use FR\BackgroundSms\BackgroundSms;

try {
    $BackgroundSms = new BackgroundSms($config);

    // Delete sent log upto given datetime inclusive
    $upto = '2020-09-09 12:00:00'; // Datetime format: Y-m-d H:i:s
    $BackgroundSms->deleteSentLog($upto);

    echo 'Sent Log Deleted';
} catch (\Exception $e) {
    $message = $e->getMessage();
    echo $message;

    pr($e);
}
