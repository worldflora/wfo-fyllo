<?php

require_once('../config.php');

// This will create a user

$username = $argv[1];
$password = $argv[2];

$password_hash = password_hash($password, PASSWORD_DEFAULT);
$mysqli->query("INSERT INTO `users` (`username`, `password_hash`) VALUES ('$username', '$password_hash');");

echo "Done\n";

         