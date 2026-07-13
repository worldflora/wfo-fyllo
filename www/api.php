<?php

require_once('../config.php');
require_once('../include/BearerToken.php');
require_once('../include/FileStore.php');
require_once('../include/NameCache.php');

set_time_limit(60 * 2); // we don't want these scripts to time out but we don't want them to block either.

$facets_cache = array(); // used to prevent calling for facets for higher level taxa repeatedly.

// this returns facet and snippet values for taxa that are provided as 'taxon graphs' in json
$post_body = file_get_contents('php://input');

if($post_body || $_GET){

    // we are serving data so check they have a bearer token that matches
    // the correct token is stored in the config secrets witht he github token and db credentials.
    if(!BearerToken::authorized()){
        http_response_code(401);
        echo "Unauthorized: You must provide a bearer token!";
        exit;
    }

    if($post_body){
        return_taxon_values(json_decode($post_body));
    }else{
        if(isset($_GET['offset']) && !isset($_GET['import'])){

            // LAST MODIFIED NAMES
            $offset = (int)$_GET['offset']; // maybe zero
            return_last_modified((double)$offset);

        }elseif(isset($_GET['metadata'])){

            // METADATA STUFF

            switch ($_GET['metadata']) {
                case 'sources':
                    return_sources_metadata(isset($_GET['since'])? $_GET['since'] : 0);
                    break;
                case 'facets':
                    return_facet_metadata(isset($_GET['since'])? $_GET['since'] : 0);
                    break;        
                case 'scores':
                    return_scores_metadata();
                    break; 
                case 'snippets':
                    return_snippets_metadata();
                    break; 
                default:
                    echo "Un-specified metadata ";
                    break;
            }
   
        }elseif(isset($_GET['import'])){

            // IMPORT STUFF

            // set up the values for the indexing - if set
            $out = (object)$_GET;

            $importer = null;
            if($out->import == 'facets' && $out->local_file_path){
                // importing facets
                require_once('../include/ImporterFacets.php');
                $importer = new ImporterFacets(
                        $out->local_file_path,
                        $out->oid,
                        $out->source_id,
                        $out->facet_value_id,
                        $out->offset
                    );
            }elseif($out->import == 'snippets' && $out->local_file_path){
                // importing snippets
                require_once('../include/ImporterSnippets.php');
                $importer = new ImporterSnippets(
                        $out->local_file_path,
                        $out->oid,
                        $out->source_id,
                        $out->offset);
            }else{
                // work out what the next import should be.
                $out = get_next_import_job($out); 
            }

            if($importer){
                if(!isset($out->page_size)) $out->page_size = 100;
                $out->rows_processed = $importer->import($out->page_size);
            }else{
                $out->rows_processed = null;
                $out->finished = null;
            }

            // flag if we have finished processing
            if($out->rows_processed != null){
                if($out->rows_processed < $out->page_size) $out->finished = true; // fewer on that page than we asked for so it was the last
                else $out->finished = false; // we processed a whole page full so there may be more
            }

            header('Content-Type: application/json');
            echo json_encode($out);
        }
    }
}else{
    // no data request so just render documentation
    render_documentation_page();
}

/**
 * We need to fetch the next import job to be done
 * 
 */
function get_next_import_job($out){

    global $mysqli;

    // nulls will come first in the list
    $sql = "SELECT `id`, `file_path`, `facet_value_id`, `oid`, `snippet_language`, `last_import` FROM sources WHERE auto_import = 1 ORDER BY last_github_check ASC, last_import ASC LIMIT 1;";
    $response = $mysqli->query($sql);
    $rows = $response->fetch_all(MYSQLI_ASSOC);
    $row = $rows[0];

    $out->import = null;
    $store = null;

    // Get the file object - will call github 
    $store = new FileStore($row['file_path']);

    if(
        !$row['last_import'] // never been imported
        ||
        (isset($store->file) && $store->file && $store->file->oid != $row['oid']) // previously imported but changed.
    ){
        // bingo we have a wrongun
        // does the file link correctly?
        if(isset($store->file) && $store->file){
            $out->remote_file_path = $row['file_path'];
            $out->source_id = $row['id'];
            $out->facet_value_id = $row['facet_value_id'];
            $out->offset = 0;
            $out->oid = $store->file->oid;
            $out->import =  $row['snippet_language'] ? 'snippets' : 'facets'; // snippets always have a language
        }else{
            error_log("GitHub didn't return file details for {$row['file_path']}");
        }
    }

    // if we have an import to do we download the file before returning to the import page
    // through mechanism
    if($store && $out->import){
        
        $local_file_dir = "../data/session_data/api";
        @mkdir($local_file_dir, 0777,true);

        // we've got a store and a local file we have downloaded.
        $out->local_file_path =  $store->downloadFile($local_file_dir);
    
    }

    // no matter what we do we mark this as having been checked on github so we don't check it again till after the others
    // don't update the modified timestamp - or we keep reindexing the source!
    $response = $mysqli->query("UPDATE `sources` SET `last_github_check` = NOW(), modified = modified WHERE `id` = {$row['id']};");

    return $out;

}

function return_taxon_values($taxon_graphs){

    $out = array();

    $limiter = 0;
    foreach ($taxon_graphs as $taxon_graph) {
        $out[] = get_taxon_values($taxon_graph);
        $limiter++;
        if($limiter > 1000) break;
    }

    header('Content-Type: application/json');
    
    // add a label 
    $out = array(
        'kind' => 'taxon-values',
        'docs' => $out
    );
    
    echo json_encode($out);
    exit;

}

/**
 * The function where we actually do the 
 * business of fetching the values.
 * 
 */
function get_taxon_values($graph){

    $doc = (object)array(); // we will return this 

    // tag it with the id of the taxon we are describing.
    $doc->taxon = $graph->taxon[0];
    $doc->classification = $graph->classification;

    // BUILD THE FACETS FIRST

    $facets = array();

    // do the taxon itself
    $facets = get_facets_for_wfo_ids($graph->taxon, 'direct');

    // work down the path
    foreach ($graph->path as $anc) {
        $facets = array_merge($facets, get_facets_for_wfo_ids($anc, 'ancestor'));
    }

    // do the synonyms
    foreach ($graph->synonyms as $syn) {
        $facets = array_merge($facets, get_facets_for_wfo_ids($syn, 'synonym'));
    }

    // convert the data for the facets into a document for the taxon
    /*
        wfo-f-*_ss A faceting field. * is the db id of the facet. Contains the values of this facet for this taxon in the form wfo-fv-*. This is the id of the facet document containing the metadata for the facet (see below). Adding "_provenance_ss" to the end give the field containing the provenance in this document.
        wfo-fv-*_provenance_ss A string that can be parsed to give the provenance of the facet value scoring. * is the db id of the facet value.
        wfo-f-*_t The text of the facet values present in this taxon. Enables freetext search by facet. Not used for rendering.
    
                        [facet_id] => 5
                    [facet_name] => Life Form
                    [heritable] => 1
                    [facet_value_id] => 1887
                    [facet_value_name] => Annual
                    [source_id] => 1554
                    [source_name] => WCVP Life Form: Annual
                    [scored_via] => synonym
                    [wfo_id] => wfo-0000540604
        */

    foreach ($facets as $facet) {

        // is there a field to hold the data for this facet?
        $facet_field_name = "wfo-f-{$facet['facet_id']}_ss";
        $provenance_field_name = "wfo-fv-{$facet['facet_value_id']}_provenance_ss";
        $text_field_name = "wfo-f-{$facet['facet_id']}_t";

        if(!isset($doc->{$facet_field_name})){
            $doc->{$facet_field_name} = array();
            $doc->{$provenance_field_name} = array();
            $doc->{$text_field_name} = $facet['facet_name'] . " : ";
        }

        // if we haven't added it already add the facet value
        $facet_value_tag = 'wfo-fv-' . $facet['facet_value_id'];
        if(!in_array( $facet_value_tag, $doc->{$facet_field_name})){
            $doc->{$facet_field_name}[] =  $facet_value_tag;
            $doc->{$text_field_name} .= ' ' . $facet['facet_value_name'];
        } 

        // add the provenance for this facet value
        // name_scored-source_scored_id-via a synonym/ancestor/direct

        $prov = "{$facet['wfo_id']}-s-{$facet['source_id']}-{$facet['scored_via']}";
        if(!isset($doc->{$provenance_field_name})) $doc->{$provenance_field_name} = array();
        if(!in_array($prov, $doc->{$provenance_field_name})) $doc->{$provenance_field_name}[] = $prov;

    }

    // ADD IN THE SNIPPETS
    /*
        snippet_text_categories_ss The catagory (subject) of the snippet. e.g. morphology or distribution.
        snippet_text_languages_ss The lanuage the snippet is in.
        snippet_text_name_ids_ss The WFO IDs that the snippets were attached to (maybe synonym of the taxon remember)
        snippet_text_ids_ss The ids of the snippets so that we can recover the snippet that is stored as a separate document in the
        snippet_text_bodies_txt The content of the snippets. These are not rendered but here so we can search by text.
        snippet_text_sources_ss The id of the source so we can facet on it.
    */

    $doc->snippet_text_categories_ss = array(); // the category the snippet is
    $doc->snippet_text_languages_ss = array(); // the language the snippet is in
    $doc->snippet_text_name_ids_ss = array(); // the WFO ID of the name the snippet is attached to
    $doc->snippet_text_ids_ss = array(); // the id of this snippet - used to recover the metadata for this snippet
    $doc->snippet_text_sources_ss = array(); // the id of this snippet source so we can facet on it
    $doc->snippet_text_bodies_txt = array(); // actual blocks of text 

    // add the main taxon
    add_snippets_for_wfo_id($doc, $graph->taxon);

    // add the synonyms
    foreach ($graph->synonyms as $syn) {
        add_snippets_for_wfo_id($doc, $syn);
    }

    // n.b. snippets are never inhereted so we don't add the path

    return $doc;

}

function add_snippets_for_wfo_id($doc, $wfo_ids){
        
        global $mysqli;

        $ids_string = "'" . implode("','", $wfo_ids) . "'";

        $response = $mysqli->query("SELECT 
            s.id, s.source_id, s.body, ss.`snippet_category` as 'category', ss.`snippet_language` as 'language' 
            FROM snippets as s 
            JOIN sources as ss on s.source_id = ss.id 
            WHERE s.wfo_id in ({$ids_string})
            AND (ss.do_not_index is NULL || ss.do_not_index = 0)");

        while($row = $response->fetch_assoc()){
            $doc->snippet_text_name_ids_ss[] = $wfo_ids[0]; // the WFO ID of the name the snippet is attached to
            $doc->snippet_text_categories_ss[] = $row['category']; // the category the snippet is
            $doc->snippet_text_languages_ss[] = $row['language']; // the language the snippet is in
            $doc->snippet_text_ids_ss[] = 'wfo-snippet-' . $row['id']; // the id of this snippet - used to recover the metadata (including data source) for this snippet
            $doc->snippet_text_sources_ss[] = 'wfo-ss-' . $row['source_id']; // the id of this snippet - used to recover the metadata (including data source) for this snippet
            $doc->snippet_text_bodies_txt[] = $row['body']; // actual blocks of text 
        }

}

function get_facets_for_wfo_ids($wfo_ids, $scored_via){

    global $mysqli;
    global $facets_cache;

    // if we have it cached just return that.
    // caching is by the prefered id which will be the first in the 
    // array
    if(isset($facets_cache[$wfo_ids[0]])) return $facets_cache[$wfo_ids[0]];

    // not got it so get it.

    // we look for facets joined to all the ids the name is known by
    $ids_string = "'" . implode("','", $wfo_ids) . "'";

    $sql = "SELECT 
        f.id as facet_id,
        f.`name` as facet_name,
        f.`heritable` as heritable,
        fv.id as facet_value_id, 
        fv.`name` as facet_value_name,
        s.id as source_id,
        s.`name` as source_name,
        '{$scored_via}'  as 'scored_via',
        '{$wfo_ids[0]}' as 'wfo_id'
    FROM wfo_scores as ws
    JOIN facet_values AS fv ON ws.value_id = fv.id
    JOIN facets AS f ON fv.facet_id = f.id
    JOIN sources AS s ON ws.source_id = s.id AND s.do_not_index = 0
    WHERE ws.wfo_id in ({$ids_string})
    AND (s.do_not_index != 1 OR s.do_not_index is NULL) ";

    // must be a heritable facet to be included
    if($scored_via == 'ancestor') $sql .= " AND f.heritable = 1 ";

    $response = $mysqli->query($sql);
    $facets = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();
    
    $facets_cache[$wfo_ids[0]] = $facets;

    // we flush the cache cache at 10,000
    if(count($facets_cache) > 10000){
        //echo "\nEmpting facets cache\n";
        $facets_cache = array();
    } else{
        // echo "\nCache". count($facets_cache) ."\n";
    }

    // finally return the goods
    return $facets;

}


function return_sources_metadata($since){
   
    global $mysqli;

    $solr_docs = array();

    $just_now = (new DateTimeImmutable())->format('Y-m-d\TH:i:s\Z'); 

    if(preg_match('/^[0-9]+\.[0-9]+$/', $since)){
        list($modified_stamp, $id) = explode('.', $since); 
    }else{
        // we are sent zero to start indexing from scratch
        // or they just didn't supply a decimal
        $modified_stamp = $since;
        $id = 0;
    }
 
    // we create solr docs as near as damn it 
    // in the query
    $response = $mysqli->query("SELECT 
        `id`,
        `name`,
        `description`,
        `link_uri`,
        `file_path` as 'git_file_path',
        `oid` as 'git_file_oid',
        `facet_value_id`,
        `snippet_category` as 'category',
        `snippet_language` as 'language',
        `last_import`,
         concat_ws('.', UNIX_TIMESTAMP(`modified`), id) as 'last_modified_d'
        FROM sources 
        WHERE do_not_index = 0
        AND (`modified` > FROM_UNIXTIME($modified_stamp) OR (`modified` = FROM_UNIXTIME($modified_stamp) AND id > $id ))
        ORDER BY modified, id");
    $sources = $response->fetch_All(MYSQLI_ASSOC);
    $response->close();

    $just_now = (new DateTimeImmutable())->format('Y-m-d\TH:i:s\Z');

    foreach ($sources as $s) {

        // are we doing a snippet or a facet source
        if($s['category']){
            $solr_docs[] = (object)array(
                'id'=> 'wfo-ss-' . $s['id'],
                'kind_s' => 'wfo-snippet-source',
                'last_modified_d' => (double)$s['last_modified_d'],
                'fyllo_last_indexed_dt' => $just_now, // useful to have the last mod as a date uniform across all solr docs.
                'json_t' => json_encode((object)$s)
            );
        }else{
            $solr_docs[] = (object)array(
                'id'=> 'wfo-fs-' . $s['id'],
                'kind_s' => 'wfo-facet-source',
                'last_modified_d' => (double)$s['last_modified_d'],
                'fyllo_last_indexed_dt' => $just_now, // useful to have the last mod as a date uniform across all solr docs.
                'json_t' => json_encode((object)$s)
            );
        }

    }

    header('Content-Type: application/json');

    // add a label 
    $out = array(
        'kind' => 'sources-metadata',
        'docs' => $solr_docs
    );
    
    echo json_encode($out);
    exit;    


}

function return_facet_metadata($since){
        
        global $mysqli;

        $solr_docs = array();

        // we create solr docs as near as damn it 
        // in the query
        $response = $mysqli->query("SELECT 
            id as db_id,
            concat('wfo-f-', id) as id,
            'wfo-facet' as kind, 
            `name` as 'name', 
            `description` as 'description',
            `link_uri` as 'link_uri',
            concat_ws('.', UNIX_TIMESTAMP(`modified`), id) as 'modified'
            FROM facets ORDER BY `name`");
        $facets = $response->fetch_All(MYSQLI_ASSOC);
        $response->close();

        $just_now = (new DateTimeImmutable())->format('Y-m-d\TH:i:s\Z');

        foreach($facets as $facet){

            // hold on to the db id
            $facet_id = $facet['db_id'];
            unset($facet['db_id']);

            // we tag with the last mod date in the facets and facet_values
            // not the mod time stamp includes the primary key as the double
            // part of the double because imports may be faster than a second
            $last_modified = (double)$facet['modified']; 
            
            // add the facet values for this facet
            $response = $mysqli->query("SELECT 
                concat('wfo-fv-', id) as id,
                'wfo-facet-value' as kind, 
                `name` as 'name', 
                `description` as 'description',
                `link_uri` as 'link_uri',
                `code` as 'code',
                concat('wfo-f-', `facet_id`) as facet_id,
                concat_ws('.', UNIX_TIMESTAMP(`modified`), id) as 'modified'
                FROM facet_values 
                WHERE facet_id = $facet_id
                ORDER BY `name`");
            $facet_values = $response->fetch_All(MYSQLI_ASSOC);
            $facet['facet_values'] = array();
            foreach ($facet_values as $fv) {
               $facet['facet_values'][$fv['id']] = $fv;
               if((double)$fv['modified'] > $last_modified) $last_modified = (double)$fv['modified'];
            }
            $response->close();
            
            // we only add it if it is modified since
            // yes I know we fetched them all but the work is done
            // on the other end inserting them in the index and we
            // don't want to do that if we don't have to.
            if($last_modified > $since){
                $solr_docs[] = (object)array(
                    'id'=> $facet['id'],
                    'kind_s' => 'wfo-facet',
                    'last_modified_d' => $last_modified,
                    'fyllo_last_indexed_dt' => $just_now, // useful to have the last mod as a date uniform across all solr docs.
                    'json_t' => json_encode((object)$facet)
                    );
            }

        }

        header('Content-Type: application/json');

        // add a label 
        $out = array(
            'kind' => 'facets-metadata',
            'docs' => $solr_docs
        );

        echo json_encode($out);
        exit;

}

function return_scores_metadata(){
        
        global $mysqli;

        set_time_limit(120); // this can be slow

        $just_now = (new DateTimeImmutable())->format('Y-m-d\TH:i:s\Z'); 

        $solr_docs = array();

        // if they haven't set a since the we begin at the start of the epoch
        if(!isset($_GET['since'])){
            $modified_stamp = 0;
            $id = 0;
        }else{
            if(preg_match('/^[0-9]+\.[0-9]+$/', $since)){
                list($modified_stamp, $id) = explode('.', $since); 
            }else{
                // we are sent zero to start indexing from scratch
                // or they just didn't supply a decimal
                $modified_stamp = $since;
                $id = 0;
            }
        } 

        $sql = "SELECT 
            `wfo_scores`.*, concat(UNIX_TIMESTAMP(`modified`), '.', id) as last_modified_d  
            FROM `wfo_scores` 
            WHERE (`modified` > FROM_UNIXTIME($modified_stamp) OR (`modified` = FROM_UNIXTIME($modified_stamp) AND id > $id ))
            AND meta_json is not null 
            ORDER BY modified, id
            LIMIT 5000;"; 
        $response = $mysqli->query($sql, MYSQLI_USE_RESULT); // we allow for big result set

        $solr_docs = array();
        while($row = $response->fetch_assoc()){

            $solr_doc = array(
                'id' => "wfo-fvs-{$row['wfo_id']}-{$row['source_id']}-{$row['value_id']}",
                'kind_s' => 'wfo-facet-value-score',
                'wfo_id_s' => $row['wfo_id'],
                'source_id_s' => $row['source_id'],
                'value_id_s' => $row['value_id'],
                'last_modified_d' => (double)$row['last_modified_d'],
                'last_modified' => $row['modified'],
                'fyllo_last_indexed_dt' => $just_now, // useful to have the last mod as a date uniform across all solr docs.
                'json_t' => $row['meta_json']
            );

            $solr_docs[] = $solr_doc;

        }

        header('Content-Type: application/json');

        // add a label 
        $out = array(
            'kind' => 'scores-metadata',
            'modified-stamp' => $modified_stamp,
            'docs' => $solr_docs
        );

        echo json_encode($out);
        exit;

}

function return_snippets_metadata(){
        
        global $mysqli;

        $just_now = (new DateTimeImmutable())->format('Y-m-d\TH:i:s\Z'); 

        $solr_docs = array();

        // if they haven't set a since the we begin at the start of the epoch
        if(!isset($_GET['since'])){
            $modified_stamp = 0;
            $id = 0;
        }else{
            list($modified_stamp, $id) = explode('.', $_GET['since']); 
        } 
        $sql = "SELECT 
            `snippets`.*,  
            concat_ws('.', UNIX_TIMESTAMP(`modified`), id) as 'last_modified_d' 
            FROM `snippets`
            WHERE (`modified` > FROM_UNIXTIME($modified_stamp) OR (`modified` = FROM_UNIXTIME($modified_stamp) AND id > $id ))
            AND meta_json is not null 
            ORDER BY modified, id LIMIT 1000;"; 

        //echo $sql; exit;
        $response = $mysqli->query($sql, MYSQLI_USE_RESULT); // we allow for big result set

        $solr_docs = array();
        while($row = $response->fetch_assoc()){

            $solr_doc = array(
                'id' => "wfo-snippet-{$row['id']}",
                'kind_s' => 'wfo-snippet',
                'wfo_id_s' => $row['wfo_id'],
                'source_id_s' => $row['source_id'],
                'last_modified_d' => (double)$row['last_modified_d'],
                'fyllo_last_indexed_dt' => $just_now, // useful to have the last mod as a date uniform across all solr docs.
                'json_t' => $row['meta_json']
            );

            $solr_docs[] = $solr_doc;

        }

        header('Content-Type: application/json');

        // add a label 
        $out = array(
            'kind' => 'snippets-metadata',
            'modified-stamp' => $modified_stamp,
            'docs' => $solr_docs
        );

        echo json_encode($out);
        exit;

}

function return_last_modified($offset){

    global $mysqli;
    $sql = "SELECT wfo_id, DATE_FORMAT(modified, '%Y-%m-%dT%TZ') as 'modified' FROM modification_log order by modified desc, wfo_id desc limit 1000;";

    //echo $sql;
    $response = $mysqli->query($sql);
    $rows = $response->fetch_all(MYSQLI_ASSOC);
    
    header('Content-Type: application/json');

    // add a label 
    $out = array(
        'kind' => 'modified_names',
        'docs' => $rows
    );

    echo json_encode($out);
    exit;

}

function render_documentation_page(){
    require_once('header.php');
?>

<p><a href="index.php">Fyllo</a> → API</p>
<h1>API for the indexer</h1>
<p class="lead">
    This is how the indexer calls Fyllo to get values for inclusion in the portal.
    It is not a public API but it is good to have an appreciation of the underlying mechanism.
</p>
<p>
    Remember that Fyllo doesn't know anything about the classification of the names it tracks.
    Actually it knows nothing about the names either! It just stores the WFO IDs and calls 
    to the public GraphQL API to render a human readable version.
    The <a href="taxa.php">Taxa</a> page likewise just calls the GraphQL API to calculate
    the values that would be stored in the index for a particular taxon. This is why it
    displays the classification used at the top of the page.
</p>

<h2>Get modified names</h2>
<p>
    Calling this URL with method GET and the parameter 'offset' will return a JSON array containing the WFO IDs of the 1,000 names with most recently changed values, and their modification date, in descending order.
    In order to perform a delta update a client can call <strong>api.php?offset=0</strong> followed by <strong>api.php?offset=1000</strong> etc until it reaches its last sync date or the supply stops.
</p>

<h2>Fetch values for taxon trees</h2>
<p>
    POSTing to this URL with the body containing a JSON array of objects describing <strong>taxon graphs</strong> will return the index values for each of the 
    taxon graphs in the array. There is a limit of 1,000 objects in each call. 
</p>
<p>
    A <strong>taxon graph</strong> is simple structure. Each name is represented by an array of WFO IDs. Nearly always this will contain a single WFO ID but sometimes 
    it will contain multiple IDs when there has been deduplication of records. Fyllo won't know about this because it just gets given data tagged with WFO IDs
    and those might be the IDs for a name record that has been merged into another.
<code>
<pre>
[
    {
        "classification": "9999-01",
        "taxon": [
            "wfo-0000632146"
        ],
        "path": [
            [
                "wfo-9971000003"
            ],
            [
                "wfo-4100001250"
            ],
            [
                "wfo-4100003335"
            ],
            [
                "wfo-9949999999",
                "wfo-9499999999"
            ],
            [
                "wfo-9000000022"
            ],
            [
                "wfo-7000000036"
            ],
            [
                "wfo-4000010286"
            ]
        ],
        "synonyms": [
            [
                "wfo-0000431439"
            ],
            [
                "wfo-0000540588"
            ],
            [
                "wfo-0000540624"
            ],
            [
                "wfo-0000540650"
            ],
            [
                "wfo-0000540651"
            ],
            [
                "wfo-0000540653"
            ]
        ]
    }
]
</pre>
</code>
</p>

<p>
   The return structure is similar to that required to update a SOLR index.
<code>
<pre>
[
    {
        "taxon": "wfo-0000632146",
        "classification": "9999-01",
        "wfo-f-2_ss": [
            "wfo-fv-52",
            "wfo-fv-72",
            "wfo-fv-182"
        ],
        "wfo-fv-52_provenance_ss": [
            "wfo-0000632146-s-60-direct",
            "wfo-0000632146-s-64-direct",
            "wfo-0000632146-s-65-direct"
        ],
        "wfo-f-2_t": "Countries (ISO) :  Chile [CL] Ecuador [EC] Peru [PE]",
        "wfo-fv-72_provenance_ss": [
            "wfo-0000632146-s-87-direct"
        ],
        "wfo-fv-182_provenance_ss": [
            "wfo-0000632146-s-192-direct"
        ],
        "wfo-f-8_ss": [
            "wfo-fv-407",
            "wfo-fv-409",
            "wfo-fv-453",
            "wfo-fv-489",
            "wfo-fv-601"
        ],
        "wfo-fv-407_provenance_ss": [
            "wfo-0000632146-s-1109-direct"
        ],
        "wfo-f-8_t": "TDWG Botanical Area :  Chile Central Chile North Gal Juan Fern Peru",
        "wfo-fv-409_provenance_ss": [
            "wfo-0000632146-s-1111-direct"
        ],
        "wfo-fv-453_provenance_ss": [
            "wfo-0000632146-s-1155-direct"
        ],
        "wfo-fv-489_provenance_ss": [
            "wfo-0000632146-s-1191-direct"
        ],
        "wfo-fv-601_provenance_ss": [
            "wfo-0000632146-s-1303-direct"
        ],
        "wfo-f-5_ss": [
            "wfo-fv-1887"
        ],
        "wfo-fv-1887_provenance_ss": [
            "wfo-0000632146-s-1554-direct",
            "wfo-0000540650-s-1554-synonym"
        ],
        "wfo-f-5_t": "Life Form :  Annual",
        "snippet_text_categories_ss": [
            "link-out"
        ],
        "snippet_text_languages_ss": [
            "zzz"
        ],
        "snippet_text_name_ids_ss": [
            "wfo-0000632146"
        ],
        "snippet_text_ids_ss": [
            "wfo-snippet-21500"
        ],
        "snippet_text_sources_ss": [
            "wfo-ss-1803"
        ],
        "snippet_text_bodies_txt": [
            "https:\/\/www.ncbi.nlm.nih.gov\/Taxonomy\/Browser\/wwwtax.cgi?id=3026891"
        ]
    }
]
</pre>
</code>
</p>

<h2>Facet and Source metadata</h2>
<p>
    The metadata for the Facets and Sources are stored as separate documents in the index, not in every taxon record.
    There is therefore a call to retrieve this data for update. It is done as a single call, no paging, as it shouldn't get that big.
</p>
<p>
    <strong>api.php?metadata=facets</strong>
</p>
<p>
    <strong>api.php?metadata=sources</strong>
</p>

<h2>Scores and Snippets</h2>
<p>
    We also need to add the extended metadata for all the Facet Value scores and the Snippets. 
    These are done with the following calls. They accept a "since" parameter as a Unix time stamp 
    so you don't need to re-index everything everytime.
    A maximum of 1,000 results are returned ordered by modified date. You can page up to the current
    date by simply calling again with since the last modified date. (You'll need to convert it to a timestamp.)
</p>
<p>
    <strong>api.php?metadata=scores&since=1774880981</strong>
</p>
<p>
    <strong>api.php?metadata=snippets&since=1774880981</strong>
</p>

<h2>Authentication & authorisation</h2>
<p>
    These API calls can be expensive to serve. We don't want a bot to get in here and start scraping stuff and so all calls require a key value in the header to be processed.
    Keys are manually configured and stored in the configuration file. There is a test script in the code that shows how the bearer token can be passed.
</p>

<?php
    require_once('footer.php');
}

?>


