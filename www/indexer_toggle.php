<?php

require_once('../config.php');
require_once('../include/IndexerSwitch.php');

// create a user object for use all over
$user = @$_SESSION['user'] ? $_SESSION['user'] : null;
    
if(!$user){
    header("HTTP/1.1 403 Forbidden");
    echo '<h1>403 Forbidden</h1>';
    echo "<p>You can't access this unless you are logged in.</p>";
    exit;
}

// flick the switch
IndexerSwitch::toggle();

// send them back where they came from.
header("Location: {$_SERVER['HTTP_REFERER']}");

