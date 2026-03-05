<?php


require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

WfoFacets::indexSnippets();
