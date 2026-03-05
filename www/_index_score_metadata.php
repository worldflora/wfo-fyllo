<?php


require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

set_time_limit(60*10); // ten minutes - if it takes longer then do it with the command line script

WfoFacets::indexScores();

header('Location: sources.php');