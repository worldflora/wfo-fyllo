<?php

// called to index a single name
// redirects after call

// FIXME - MUST BE GOD TO DO THIS

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

WfoFacets::indexFacets();

header('Location: facets.php');