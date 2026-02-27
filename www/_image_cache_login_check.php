<?php

    require_once('../config.php');

    // this is called directly from the image cache to validate
    // a login key.

    // We always return json
    header('Content-Type: application/json');

    $out = (object)array(
        'key' => null,
        'message' => null,
        'success' => false
    );
    
    // no key so fail
    if(!isset($_POST['key'])){
        $out->success = false;
        $out->message = "FAILED: No key passed";
        echo json_encode($out, JSON_PRETTY_PRINT);
        exit;
    }

    $keys_path = '../data/image_cache_keys/'; // end in slash

    // make the directory if it doesn't exist
    if(!file_exists( $keys_path )) mkdir( $keys_path , 0777, true);

    // remove any keys older than 1 minute (60 seconds) to prevent proliferation
    // and use of expired keys
    $files = glob($keys_path . '*.key');
    $now = time();
    foreach ($files as $file) {
        if($now - filemtime($file) > 60) unlink($file);
    }
    
    $key = $_POST['key'];
    $out->key = $key;
    $key_file = '../data/image_cache_keys/' . $key . '.key';

    if(file_exists($key_file)){
        // they have a good key
        unlink($key_file); // remove it as it is single use
        $out->success = true;
        $out->message = "SUCCESS: Key valid.";
    }else{
        // they don't have a good key
        $out->success = false;
        $out->message = 'FAILED: key ' . $key . ' does not exist';
    }
    
    echo json_encode($out, JSON_PRETTY_PRINT);
