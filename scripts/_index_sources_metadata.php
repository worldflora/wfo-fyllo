<?php

// called to index a single name
// redirects after call

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');

echo "\nIndexing Facet Sources\n";
WfoFacets::indexFacetSources();

echo "\nIndexing Snippet Sources\n";
WfoFacets::indexSnippetSources();


