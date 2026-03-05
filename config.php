<?php

/*

    configuration that is safe to include in 
    github

*/

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
//error_reporting(E_ALL);
session_start();

require_once('../../wfo_facet_secrets.php'); // things we don't put in github

// Location of the solr server - we should not know about SOLR!
//define('SOLR_QUERY_URI', $solr_query_uri); // from wfo_secrets.php
//define('SOLR_USER', $solr_user); // from wfo_secrets.php
//define('SOLR_PASSWORD', $solr_password); // from wfo_secrets.php

define('PLANT_LIST_GRAPHQL_URI', $plant_list_graphql_uri);

// the image cache uses us as a login so we need a redirect 
//define('IMAGE_CACHE_LOGIN_URI', 'http://localhost:1966/login');
define('IMAGE_CACHE_LOGIN_URI', 'https://wfo-image-cache.rbge.info/remote');

// used for lookups and other services that don't want to 
// trouble themselves with many versions of backbone
// will normally be set to the most recent.
define('WFO_DEFAULT_VERSION','9999-04');

// where the SQLite files are stored for export of lists end it in a slash
define('WFO_EXPORTS_DIRECTORY','../data/exports/');


// create and initialise the database connection
$mysqli = new mysqli($db_host, $db_user, $db_password, $db_database);  

// connect to the database
if ($mysqli->connect_error) {
  echo $mysqli->connect_error;
}

if (!$mysqli->set_charset("utf8mb4")) {
  echo printf("Error loading character set utf8: %s\n", $mysqli->error);
}

// definitions of export types available
$export_formats = array();
$export_formats[] = (Object)array(
    'id' => 'csv', 'name' => 'CSV Table', 'description' => 'Comma separated values in a UTF-8 encoded text file.'
);
$export_formats[] = (Object)array(
    'id' => 'html', 'name' => 'HTML document', 'description' => 'The list will be displayed as a hierachical list suitable for proof reading.'
);
$export_formats[] = (Object)array(
    'id' => 'coldp', 'name' => 'Catalogue of Life Data Package', 'description' => 'A data package suitable for submission to GBIF ChecklistBank'
);