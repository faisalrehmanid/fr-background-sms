<?php

namespace FR\BackgroundSms\Helper;

class Util
{
    /**
     * Filter given mobile number
     *
     * @param string $mobile_number
     * @return string
     */
    public static function filterMobileNumber($mobile_number)
    {
        $mobile_number = str_replace(" ", "", $mobile_number);
        $mobile_number = str_replace("+", "", $mobile_number);
        $mobile_number = str_replace("-", "", $mobile_number);
        $mobile_number = str_replace("/", "", $mobile_number);
        $mobile_number = str_replace(",", "", $mobile_number);
        $mobile_number = str_replace(".", "", $mobile_number);

        $start = substr($mobile_number, 0, 4);
        if ($start == "0092")
            $mobile_number = substr($mobile_number, 4, strlen($mobile_number));

        $start = substr($mobile_number, 0, 3);
        if ($start == "092")
            $mobile_number = substr($mobile_number, 3, strlen($mobile_number));

        $start = substr($mobile_number, 0, 2);
        if ($start == "03")
            $mobile_number = substr($mobile_number, 1, strlen($mobile_number));

        $start = substr($mobile_number, 0, 4);
        if ($start == "9203")
            $mobile_number = substr($mobile_number, 3, strlen($mobile_number));

        $start = substr($mobile_number, 0, 4);
        if ($start == "9292")
            $mobile_number = substr($mobile_number, 4, strlen($mobile_number));

        $start = substr($mobile_number, 0, 5);
        if ($start == "92210")
            $mobile_number = substr($mobile_number, 5, strlen($mobile_number));

        $start = substr($mobile_number, 0, 4);
        if ($start == "9221")
            $mobile_number = substr($mobile_number, 4, strlen($mobile_number));

        $start = substr($mobile_number, 0, 5);
        if ($start == "92420")
            $mobile_number = substr($mobile_number, 5, strlen($mobile_number));

        $start = substr($mobile_number, 0, 4);
        if ($start == "9242")
            $mobile_number = substr($mobile_number, 4, strlen($mobile_number));

        $start = substr($mobile_number, 0, 2);
        if ($start != "92")
            $mobile_number = "92" . $mobile_number;

        if (strlen($mobile_number) != 12 || substr($mobile_number, 0, 3) != '923')
            return '';

        return $mobile_number;
    }

    /**
     * Validate single email address and convert email string to array 
     * when email used with name. Email and name separated by :
     * 
     * @param string $email e.g. some1@address.com: Name 
     * @return string | array e.g. [some1@address.com => Name]
     */
    public static function emailToArray($email)
    {
        $parts = explode(':', $email);
        @$email = strtolower(trim($parts[0])); // Email
        @$name  = trim($parts[1]); // Name

        if (filter_var($email, FILTER_VALIDATE_EMAIL) && !empty($email)) {
            if (!empty($name))
                return [$email => $name];
            else
                return $email;
        }

        return '';
    }

    /**
     * Convert multiple emails string to array
     * Multiple emails separated by ; and each email and name separated by :
     * 
     * @param string $emails e.g. some1@address.com; some2@address.com: Name; some3@address.com
     * @return array e.g [  some1@address.com, 
     * 					   [some2@address.com => Name],
     * 						some3@address.com
     * 					 ]
     */
    public static function emailsToArray($emails)
    {
        $array = [];
        $emails = explode(';', trim(trim($emails), ';'));

        foreach ($emails as $k => $email) {
            $email = self::emailToArray($email);
            if (!empty($email)) {
                if (is_array($email))
                    $array = array_merge($array, $email);
                else
                    $array[] = $email;
            }
        }

        return $array;
    }

    /**
     * Validate single email address
     *
     * @param string $email
     * @return string
     */
    public static function validateEmail($email)
    {
        $email = strtolower(trim($email));
        if (filter_var($email, FILTER_VALIDATE_EMAIL) && !empty($email)) {
            return $email;
        }

        return '';
    }

    /**
     * Send email 
     *
     * @param string $from Single email address like from@test.com:From Name
     * @param string $smtp_json SMTP Json string
     * @param string $subject Email subject
     * @param string $body Email body
     * @param string $to Multiple emails separated by ; like email1@test.com:Email1;email2@test.com;
     * @param string $reply_to Multiple emails separated by ; like email1@test.com:Email1;email2@test.com;
     * @param string $cc Multiple emails separated by ; like email1@test.com:Email1;email2@test.com;
     * @param string $bcc Multiple emails separated by ; like email1@test.com:Email1;email2@test.com;
     * @return bool
     */
    public static function sendEmail(
        $from,
        $smtp_json,
        $subject,
        $body,
        $to,
        $reply_to,
        $cc,
        $bcc
    ) {
        @$from      = self::emailToArray($from);
        @$smtp      = json_decode($smtp_json, true);
        @$subject   = trim($subject);
        @$body      = trim($body);
        @$to        = self::emailsToArray($to);
        @$reply_to  = self::emailsToArray($reply_to);
        @$cc        = self::emailsToArray($cc);
        @$bcc       = self::emailsToArray($bcc);

        try {
            if (empty($smtp))
                $SwiftTransport = new \Swift_SendmailTransport();

            if (!empty($smtp)) {
                $SwiftTransport = new \Swift_SmtpTransport();

                if (@$smtp['host']) $SwiftTransport->setHost($smtp['host']);
                if (@$smtp['port']) $SwiftTransport->setPort($smtp['port']);
                if (@$smtp['encryption']) $SwiftTransport->setEncryption($smtp['encryption']);
                if (@$smtp['username']) $SwiftTransport->setUsername($smtp['username']);
                if (@$smtp['password']) $SwiftTransport->setPassword($smtp['password']);
            }

            $SwiftMailer  = new \Swift_Mailer($SwiftTransport);
            $SwiftMessage = new \Swift_Message();

            if (!empty($from))
                $SwiftMessage->setFrom($from);
            else
                throw new \Exception('From email could not be empty');

            if (!empty($subject))
                $SwiftMessage->setSubject($subject);
            else
                throw new \Exception('Subject could not be empty');

            if (!empty($body)) {
                $SwiftMessage->setBody($body, 'text/html');
                $SwiftMessage->addPart(strip_tags($body), 'text/plain');
            } else
                throw new \Exception('Body could not be empty');

            if (!empty($to))
                $SwiftMessage->setTo($to);
            else
                throw new \Exception('To email could not be empty');

            if (!empty($reply_to))
                $SwiftMessage->setReplyTo($reply_to);

            if (!empty($cc))
                $SwiftMessage->setCc($cc);

            if (!empty($bcc))
                $SwiftMessage->setBcc($bcc);

            $SwiftMailer->send($SwiftMessage);

            return true;
        } catch (\Exception $e) {
            error_log("Caught $e");

            return false;
        }
    }

    /**
     * Generate Unique ID of fixed length
     *
     * @param int $length
     * @return string
     */
    public static function generateUniqueId($length = 32)
    {
        $length = intval($length) / 2;
        if ($length == 0) return '';

        if (function_exists('random_bytes')) {
            $random = random_bytes($length);
        }

        if (function_exists('openssl_random_pseudo_bytes')) {
            $random = openssl_random_pseudo_bytes($length);
        }

        if ($random !== false && strlen($random) === $length) {
            return  bin2hex($random);
        }

        $unique_id = '';
        $characters = '0123456789abcdef';
        for ($i = 0; $i < ($length * 2); $i++) {
            $unique_id .= $characters[rand(0, strlen($characters) - 1)];
        }

        return $unique_id;
    }

    /**
     * Parse `gearadmin --status` command line output
     *
     * @param string $gearadmin cmd command complete path e.g: /bin/gearadmin
     * @return array of statuses
     */
    public static function parseGearadminStatus($gearadmin)
    {
        // Save command output
        $output = [];
        // Execute command to get all gearman job statuses
        $command = $gearadmin . " --status";
        exec($command, $output);

        if (!empty($output)) {
            $statuses = []; // Parse each string and save in $statuses array

            // Parse gearadmin --status $output
            foreach ($output as $k => $row) {
                // Flag to identify next column
                $space = 1;

                // Loop through all characters in each row
                for ($i = 0; $i < strlen($row); $i++) {
                    $chr = $row[$i];
                    // Replace any tab char with space
                    if ($chr == "\t") {
                        $chr = " ";
                    }

                    if (trim($chr) != '') {
                        if ($space == 1)
                            @$statuses[$k]['function'] .= $chr; // Function name
                        if ($space == 2)
                            @$statuses[$k]['queue'] .= $chr;    // Number of jobs in queue
                        if ($space == 3)
                            @$statuses[$k]['running'] .= $chr;  // Number of jobs running
                        if ($space == 4)
                            @$statuses[$k]['workers'] .= $chr;  // Number of capable workers 
                    } else {
                        $space++;
                    }
                }
            }

            return $statuses;
        }

        return [];
    }

    /**
     * Drop all idle gearman functions that doing nothing
     * Which means functions having 0 queue, 0 running, 0 workers 
     *
     * It is a safe function and only drop that function having 0 0 0
     * when run `gearadmin --stauts` on command line
     *
     * @param string $gearadmin cmd command complete path e.g: /bin/gearadmin
     * @return void
     */
    public static function dropIdleGearmanFunctions($gearadmin)
    {
        $statuses = self::parseGearadminStatus($gearadmin);

        if (!empty($statuses)) {
            // Loop through all $statuses
            foreach ($statuses as $k => $status) {
                // Function name must not be empty and must not be '.'
                if ($status['function'] != '' && $status['function'] != '.') {
                    // Drop function that have no job in queue with no running job
                    // and have no running worker
                    if (
                        $status['queue'] == '0' &&
                        $status['running'] == '0' &&
                        $status['workers'] == '0'
                    ) {
                        // Execute command to drop function
                        $command = $gearadmin . ' --drop-function ' . $status['function'];
                        exec($command);
                    }
                }
            }
        }
    }

    /**
     * Shutdown sms background worker
     *
     * @param string $gearadmin cmd command complete path e.g: /bin/gearadmin
     * @param string $pkill cmd command complete path e.g: /bin/pkill
     * @param string $sms_background_worker_id 
     * @return void
     */
    public static function shutdownSmsBackgroundWorker($gearadmin, $pkill, $sms_background_worker_id)
    {
        // Remove background worker running process
        $command = $pkill . ' -f ' . $sms_background_worker_id;
        exec($command);

        // Remove send sms worker running process
        $send_sms_worker = str_replace('SmsBackgroundWorker-', 'SendSmsWorker-', $sms_background_worker_id);
        $command = $pkill . ' -f ' . $send_sms_worker;
        exec($command);

        // Drop functions related to sms_background_worker_id
        $statuses = self::parseGearadminStatus($gearadmin);

        if (!empty($statuses)) {
            $send_sms_worker_id = str_replace('SmsBackgroundWorker-', '', $sms_background_worker_id);

            // Loop through all $statuses
            foreach ($statuses as $k => $status) {
                // Drop function name that must contain send_sms_worker_id
                if (strpos($status['function'], $send_sms_worker_id) !== false) {
                    // Execute command to drop function
                    $command = $gearadmin . ' --drop-function ' . $status['function'];
                    exec($command);
                }
            }
        }
    }

    /**
     * Delete file
     *
     * @param string $file_path
     * @return void
     */
    public static function deleteFile($file_path)
    {
        if (is_file($file_path))
            unlink($file_path);
    }
}
