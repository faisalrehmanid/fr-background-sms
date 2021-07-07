<?php
require __DIR__ . '/config.php';

use FR\BackgroundSms\BackgroundSms;

try {
    $BackgroundSms = new BackgroundSms($config);

    // Create database structure if already not created
    $result = $BackgroundSms->createDBStructure();

    if ($result)
        echo 'Database structure created successfully.';
    else
        echo 'Could not create database structure. Already exists.';
} catch (\Exception $e) {
    $message = $e->getMessage();
    echo $message;

    pr($e);
}
