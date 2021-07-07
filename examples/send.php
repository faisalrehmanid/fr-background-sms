<?php
require __DIR__ . '/config.php';

use FR\BackgroundSms\BackgroundSms;

try {
    // Prepare recipients list
    // @see \FR\BackgroundSms\BackgroundSms::send() for reference

    // From Details
    $from_json = json_encode([
        'id' => 'test',
        'pass' => 'test-pass',
        'mask' => 'Test',
    ]);
    $body = 'Test message ___NAME___ ';

    $index = 0;
    $recipients = [];
    $keys = [];
    if (($handle = fopen("./to.csv", "r")) !== FALSE) {
        while (($row = fgetcsv($handle)) !== FALSE) {
            if ($index == 0) { // Read header row
                $keys = $row;
            } elseif ($index > 0) // Skip first header row
            {
                $vars = [];
                foreach ($row as $k => $v)
                    $vars[$keys[$k]] = $v;

                foreach ($vars as $key => $value)
                    $body = str_replace($key, $value, $body);

                $to = $vars['___TO___'];

                $recipients[] = [
                    "vendor_name" => "SMS4Connect",
                    "mask" => "Test",
                    "from_json" => $from_json,
                    "body" => $body,
                    "to" => $to
                ];
            }
            $index++;
        }

        fclose($handle);
    }
    // pr($recipients);

    // Sending email to $recipients
    $BackgroundSms = new BackgroundSms($config);

    // $notify_to is optional and can be empty string but when given 
    // it will send notification email about job status to given email addresses

    // Notification email will be sent when:
    //  * Job has been Started
    //  * Job has been Completed
    //  * Job has been Canceled
    $notify_to = 'faisal.rehman@test.com: Faisal Rehman; faisalrehmanid@hotmail.com;';

    // Send sms or mass sms in background
    $job_id = $BackgroundSms->send($recipients, $notify_to);

    echo '64 Chars unique job id: ' . $job_id;
} catch (\Exception $e) {
    $message = $e->getMessage();
    echo $message;

    pr($e);
}
