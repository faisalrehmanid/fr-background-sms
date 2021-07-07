<?php
require __DIR__ . '/config.php';

use FR\BackgroundSms\BackgroundSms;

try {
    $BackgroundSms = new BackgroundSms($config);

    // Get sms balance for given vendor
    $vendor_name = 'SMS4Connect';
    $from_json   = '{"id":"test","pass":"test-pass","mask":"Test"}';
    $result = $BackgroundSms->getSmsBalance($vendor_name, $from_json);

    pr($result);
} catch (\Exception $e) {
    $message = $e->getMessage();
    echo $message;

    pr($e);
}
