<?php

require_once('../config.php');
require_once('../include/WfoFacets.php');
/**
 * A root class for the application
 * 
 */
class WfoFacets{


    public static $facetsCache = array();

    public static $requiredFields = array('id', 'wfo_id_s', 'classification_id_s', 'full_name_string_html_s', 'role_s', 'parent_id_s', 'accepted_id_s', 'child_taxon_count_i', 'name_ancestor_path');
    

    /**
     * Does the full monty to index a taxon
     * taking care of the hierarchy 
     * 
     * @param since unix timestamp. Will only index 
     * documents older than this if supplied.
     * This is needed to stop a taxon being indexed for
     * everyone one of its synonyms.
     */
    public static function indexTaxon($wfo_id, $since = false, $commit = true){

        $solr_doc = WfoFacets::getTaxonIndexDoc($wfo_id, $since, $commit);
        if($solr_doc){
            if(is_object($solr_doc)){
                $index = new SolrIndex();
                $index->saveDoc($solr_doc, $commit);
                return $solr_doc;
            }else{
                return true;
            }
        }else{
            return false;
        }

    }

     public static function getTaxonIndexDoc($wfo_id, $since = false, $commit = true){

        global $mysqli;

        $index = new SolrIndex();

        // get the whole taxon tree from SOLR query
        $taxon_tree = WfoFacets::getTaxonTree($wfo_id);

        if(!$taxon_tree['target']){
            echo "Couldn't index $wfo_id !\n";// possibly duff wfo id
        }; 

        // if there is no current usage then it isn't placed
        // and we return false - no indexing
        if(!$taxon_tree['target'] || $taxon_tree['target']->role_s == 'unplaced' || $taxon_tree['target']->role_s == 'deprecated') return false;

        // check this is an accepted name.
        // if not then index the accepted name or nothing
        if($taxon_tree['target']->role_s == 'synonym'){
            // it is a synonym
            if( isset($taxon_tree['target']->accepted_id_s) &&  isset($taxon_tree['all'][$taxon_tree['target']->accepted_id_s])){
                // it has an accepted name and that name is available in the tree
                return WfoFacets::getTaxonIndexDoc($taxon_tree['all'][$taxon_tree['target']->accepted_id_s]->wfo_id_s, $since);
            }else{
                // something odd. We don't have the accepted name for the synonym
                echo "\nWe don't have the accepted taxon in the tree\n";
                exit;
                return false;
            }
        }

        // build a list of the wfo ids - for the names - from the path
        $path_wfos = array();
        foreach($taxon_tree['path'] as $p){
            $path_wfos[] = $p->wfo_id_s;
            if(isset($p->wfo_id_deduplicated_ss)){
                foreach ($p->wfo_id_deduplicated_ss as $dupe) {
                    $path_wfos[] = $dupe;
                }
            }
        } 

        // now we have all the names/taxa in order from top to bottom

        // get a copy of the document we will update and return
        $solr_doc = $taxon_tree['target'];

        // if we have set a start time and there is one in the solr doc and the 
        // solr doc is newer than the start time then don't index it - we've just done it!
        if($since && isset($solr_doc->facets_last_indexed_i) && $solr_doc->facets_last_indexed_i > $since){
            return true;
        }

        // get the scoring for each one and add it as required.
        $my_scores = array();
        $my_sources = array(); // we need to facet on the sources the data came from
        foreach($path_wfos as $wfo){


            $facets = WfoFacets::getFacetsForWfoId($wfo);

            foreach($facets as $facet){

                // is this facet heritable?
                // if it isn't heritable then we should only proceed if this
                // is actually the name in question (not one of its ancestors)
                if(!$facet['heritable'] && $wfo !== $solr_doc->wfo_id_s) continue;

                $facet_value_id = $facet['facet_id'] . '-' . $facet['facet_value_id'];

                // the provenance tag 
                $provenance_tag = "{$wfo}-s-{$facet['source_id']}-" . ($wfo == $wfo_id ? 'direct' : 'ancestor');

                // keep a handle on the sources stuff is coming from
                $my_sources[] = $facet['source_id'];

                // Is it already recorded?
                if(isset($my_scores[$facet_value_id])){
                    // add the source as an extra
                    $my_scores[$facet_value_id]['prov'][] = $provenance_tag;
                }else{
                    // setting it up for the first time so create the provenance array
                    $facet['prov'] = array($provenance_tag);
                    unset($facet['source_id']);
                    $my_scores[$facet_value_id] = $facet;
                }

            }

        }

        // now we do the direct synonyms
        foreach ($taxon_tree['synonyms'] as $syn) {

            // we have to account for deduplication ids 
            // in the synonyms
            $syn_ids = array();
            $syn_ids[] = $syn->wfo_id_s;
            if(isset($syn->wfo_id_deduplicated_ss)){
                foreach ($syn->wfo_id_deduplicated_ss as $dupe) {
                    $syn_ids[] = $dupe;
                }
            }
            
            $facets = array();

            foreach($syn_ids as $syn_id){
                $facets = array_merge($facets, WfoFacets::getFacetsForWfoId($syn_id));
            }

            foreach($facets as $facet){

                $facet_value_id = $facet['facet_id'] . '-' . $facet['facet_value_id'];
                $provenance_tag = "{$syn->wfo_id_s}-s-{$facet['source_id']}-synonym"; // only add here

                // Is it already recorded?
                if(isset($my_scores[$facet_value_id])){
                    // add the source as an extra
                    $my_scores[$facet_value_id]['prov'][] = $provenance_tag;
                }else{
                    // setting it up for the first time so create the provenance array
                    $facet['prov'] = array($provenance_tag);
                    unset($facet['source_id']);
                    $my_scores[$facet_value_id] = $facet;
                }

            }
        }

        // now we need to actually update the index
  //      echo "<pre>";
  //      print_r($my_scores);
//exit;
        // remove the existing facet properties
        foreach($solr_doc as $prop => $val){
            if(preg_match('/^wfo-f-.+_ss$/', $prop)) unset($solr_doc->{$prop}); // facet field
            if(preg_match('/^wfo-fv-.+_provenance_ss$/', $prop)) unset($solr_doc->{$prop}); // facet provenance field
            if(preg_match('/^wfo-f-.+_t$/', $prop)) unset($solr_doc->{$prop}); // text version
        }

        // actually add them in
        foreach($my_scores as $score){

            $facet_field_name = "wfo-f-{$score['facet_id']}_ss";
            $facet_prov_field_name = "wfo-fv-{$score['facet_value_id']}_provenance_ss";
            $facet_text_field_name = "wfo-f-{$score['facet_id']}_t";

            // if we don't have this facet yet then add 
            // in the required fields
            if(!isset($solr_doc->{$facet_field_name})){
                $solr_doc->{$facet_field_name} = array();
                $solr_doc->{$facet_prov_field_name} = array();
                $solr_doc->{$facet_text_field_name} = $score['facet_name'] . " : ";
            }

            // add the facet value
            $solr_doc->{$facet_field_name}[] = 'wfo-fv-' . $score['facet_value_id'];

            // add it to the provenance field
            foreach($score['prov'] as $prov){
                $solr_doc->{$facet_prov_field_name}[] = $prov;
            }
            
            // add it to the text field so we can 
            $solr_doc->{$facet_text_field_name} .= $score['facet_value_name'] . " : ";
            
        }

        // We've added the scores now we add a field with the sources
        // so we can facet on sources of data if needed
        $my_sources = array_unique($my_sources);
        $solr_doc->{'wfo-facet-sources_ss'} = array();
        foreach($my_sources as $source_id){
            $solr_doc->{'wfo-facet-sources_ss'}[] = 'wfo-fs-' . $source_id; 
        }
        
        /*
         TEXT SNIPPETS 
            - these are only done on the target and its synonyms
            - we use five multivalue fields
            We could try and use the correct language version of the field but we'd need to be sure we had the correct languages set up in index and here.
             this is an exercise left to version 2!
        */
        
        // remove the existing snippet properties
        $solr_doc->snippet_text_categories_ss = array(); // the category the snippet is
        $solr_doc->snippet_text_languages_ss = array(); // the language the snippet is in
        $solr_doc->snippet_text_name_ids_ss = array(); // the WFO ID of the name the snippet is attached to
        $solr_doc->snippet_text_ids_ss = array(); // the id of this snippet - used to recover the metadata for this snippet
        $solr_doc->snippet_text_sources_ss = array(); // the id of this snippet source so we can facet on it
        $solr_doc->snippet_text_bodies_txt = array(); // actual blocks of text 
  
        // add the target taxon
        WfoFacets::addSnippetsForWfoId($solr_doc, $wfo_id);

        // add the snippets for each of the immediate synonyms
        foreach ($taxon_tree['synonyms'] as $syn){
            WfoFacets::addSnippetsForWfoId($solr_doc, $syn->wfo_id_s);
        }

        // flag when we indexed it
        $solr_doc->facets_last_indexed_i = time();

        // absolutely refuse to index something that isn't accepted
        if($solr_doc->role_s != 'accepted'){
            echo "\nTrying to index non-accepted taxon.\n";
            exit;
        }

        return $solr_doc;

    }

    /**
     * This will fetch the facets for wfo ids
     * It acts as a cache so we aren't continually
     * retrieving the same data over and over all
     * the way up the tree
     */
    private static function getFacetsForWfoId($wfo_id){

        global $mysqli;

        // if we have it cached just return that.
        if(isset(WfoFacets::$facetsCache[$wfo_id])) return WfoFacets::$facetsCache[$wfo_id];

        // not got it so get it.
        $sql = "SELECT 
        f.id as facet_id,
        f.`name` as facet_name,
        f.`heritable` as heritable,
        fv.id as facet_value_id, 
        fv.`name` as facet_value_name,
        s.id as source_id,
        s.`name` as source_name
        FROM wfo_scores as ws
        JOIN facet_values AS fv ON ws.value_id = fv.id
        JOIN facets AS f ON fv.facet_id = f.id
        JOIN sources AS s ON ws.source_id = s.id AND s.do_not_index = 0
        WHERE ws.wfo_id = '{$wfo_id}';";

        $response = $mysqli->query($sql);
        $facets = $response->fetch_all(MYSQLI_ASSOC);
        $response->close();
        
        WfoFacets::$facetsCache[$wfo_id] = $facets;

        // we flush the cache cache at 10,000
        if(count(WfoFacets::$facetsCache) > 10000){
            echo "\nEmpting facets cache\n";
            WfoFacets::$facetsCache = array();
        } else{
           // echo "\nCache". count(WfoFacets::$facetsCache) ."\n";
        }

        // finally return the goods
        return $facets;

    }


    /**
     * Will add all the associated snippet data to the solr doc for 
     * a given wfo_id
     */
    private static function addSnippetsForWfoId($solr_doc, $wfo_id){
        
        global $mysqli;

        $response = $mysqli->query("SELECT s.id, s.source_id, s.body, ss.category, ss.`language` 
            FROM wfo_facets.snippets as s 
            JOIN snippet_sources as ss on s.source_id = ss.source_id 
            JOIN sources as sss on s.source_id = sss.id AND sss.do_not_index = 0
            WHERE s.wfo_id = '$wfo_id';");

        while($row = $response->fetch_assoc()){
            $solr_doc->snippet_text_name_ids_ss[] = $wfo_id; // the WFO ID of the name the snippet is attached to
            $solr_doc->snippet_text_categories_ss[] = $row['category']; // the category the snippet is
            $solr_doc->snippet_text_languages_ss[] = $row['language']; // the language the snippet is in
            $solr_doc->snippet_text_ids_ss[] = 'wfo-snippet-' . $row['id']; // the id of this snippet - used to recover the metadata (including data source) for this snippet
            $solr_doc->snippet_text_sources_ss[] = 'wfo-ss-' . $row['source_id']; // the id of this snippet - used to recover the metadata (including data source) for this snippet
            $solr_doc->snippet_text_bodies_txt[] = $row['body']; // actual blocks of text 
        }


    }

    // get the whole taxon tree from a two SOLR queries
    // not really a tree but a path
    public static function getTaxonTree($wfo_id){

        // FIXME - PORT THIS TO USE THE GRAPHQL API RATHER THAN CALL 
        // SOLR.

        $index = new SolrIndex();
        $tree = array();
        $tree['target']  = null;
        $tree['all'] = array();
        $tree['path'] = array();
        $tree['synonyms'] = array();
        $tree['target'] = $index->getDoc($wfo_id);
        
        // if we haven't got the target by the doc id maybe it is a deduplicated wfo_id?
        if(!$tree['target']){

            $solr_query = array(
                'query' => "wfo_id_deduplicated_ss:$wfo_id",
                'filter' => array("classification_id_s:" . WFO_DEFAULT_VERSION),
                'fields' => WfoFacets::$requiredFields
            );
            $solr_response = $index->getSolrResponse($solr_query);
            if(isset($solr_response->response->docs) && $solr_response->response->docs){
                $tree['target'] = $solr_response->response->docs[0];
            }

        }

        // still nothing found so just get out of here
        if(!$tree['target'])return $tree;

        // no structure for unplaced names
        if($tree['target']->role_s == 'unplaced' || !isset($tree['target']->name_ancestor_path)){
            return $tree;
        }

        $query = array(
            'query' => "name_ancestor_path:{$tree['target']->name_ancestor_path}", // everything in this tree of names
            "limit" => 10000, // big limit - not run out of memory theoretically could fail on stupid numbers of synonyms
            'filter' => array("classification_id_s:{$tree['target']->classification_id_s}"),// filtered by this classification
            'fields' => WfoFacets::$requiredFields
        );
        
        $docs = $index->getSolrDocs((object)$query);


        // get all the docs indexed by their ids
        foreach($docs as $doc){
            $tree['all'][$doc->id] = $doc;
        }

        // build the path
        $tree['path'][] = $tree['target'];
        while(true){
            $current = end($tree['path']);
            if(!isset($current->parent_id_s)) break; // reached the end
            if($current->id == $current->parent_id_s) break;
            $tree['path'][] = $tree['all'][$current->parent_id_s];
        }

        $tree['path'] = array_reverse($tree['path']);

        // build the synonyms
        foreach ($tree['all'] as $syn) {
            if(isset($syn->accepted_id_s) && $syn->accepted_id_s == $tree['target']->id){
                $tree['synonyms'][] = $syn;
            }
        }
        
        return $tree;


    }


    /*
    should be in airflow
    public static function indexFacetSources(){

        global $mysqli;

        $solr_docs = array();

        // we create solr docs as near as damn it 
        // in the query
        $response = $mysqli->query("SELECT 
            concat('wfo-fs-', id) as id,
            'wfo-facet-source' as kind, 
            `name` as 'name', 
            `description` as 'description',
            `link_uri` as link_uri,
            `harvest_uri` as harvest_uri
            FROM sources ORDER BY `name`");
        $sources = $response->fetch_All(MYSQLI_ASSOC);
        $response->close();

        foreach ($sources as $s) {
            $solr_docs[] = (object)array('id'=> $s['id'], 'kind_s' => 'wfo-facet-source', 'json_t' => json_encode((object)$s));
        }

        $index = new SolrIndex();
        $response = $index->saveDocs($solr_docs, true);
     //   echo "<pre>";
     //   print_r($solr_docs);

    }
     */

    /*
    public static function indexSnippetSources(){

        global $mysqli;

        $solr_docs = array();

        // we create solr docs as near as damn it 
        // in the query
        $response = $mysqli->query("SELECT 
                concat('wfo-ss-', s.id) as id,
                'wfo-snippet-source' as kind, 
                s.`name` as 'name', 
                s.`description` as 'description',
                s.`link_uri` as 'link_uri',
                ss.`category` as 'category',
                ss.`language` as 'language'
            FROM sources as s
            JOIN snippet_sources AS ss on ss.source_id = s.id
            ORDER BY s.`name`");
        $sources = $response->fetch_All(MYSQLI_ASSOC);
        $response->close();

        foreach ($sources as $s) {
            $solr_docs[] = (object)array(
                'id'=> $s['id'],
                'kind_s' => $s['kind'],
                'json_t' => json_encode((object)$s)
            );
        }

        //print_r($solr_docs);
        $index = new SolrIndex();
        $response = $index->saveDocs($solr_docs, true);
        //print_r($response);
     //   echo "<pre>";
     //   print_r($solr_docs);

    }

    */
    public static function getFacetsFromDoc($solrDoc){

        $index = new SolrIndex();
        $out = array();

        foreach($solrDoc as $prop => $val){
            $matches = array();
            if(preg_match('/^(wfo-f-[0-9]+)_s/', $prop, $matches)){

                // set up the facet
                $prop_prefix = $matches[1];
                $out[$prop_prefix] = array();
                $out[$prop_prefix]['facet_values'] = array();

                // add the values
                foreach ($solrDoc->{$prop} as $fv) {
                    $out[$prop_prefix]['facet_values'][$fv] = array();
                    $out[$prop_prefix]['facet_values'][$fv]['provenance'] = array();

                    // and their provenance 
                    $prov_prop = $fv . '_provenance_ss';
                    foreach($solrDoc->{$prov_prop} as $prov){
                        $out[$prop_prefix]['facet_values'][$fv]['provenance'][]  = $prov;
                    }

                }
                
            }
        } // fin building the structure

        // if we've not been indexed then we are empty
        if(!$out) return $out;

        // populate it with names
        $query = array(
            'query' => "id:(" . implode(' OR ', array_keys($out)) . ")"
            );
        $facet_docs = $index->getSolrDocs((object)$query);
        foreach ($facet_docs as $fd){
           $meta = json_decode($fd->json_t);

           $out[$fd->id]['meta']['id'] = $meta->id;
           $out[$fd->id]['meta']['name'] = $meta->name;
           $out[$fd->id]['meta']['description'] = trim($meta->description ?? '');
           $out[$fd->id]['meta']['link_uri'] = trim($meta->link_uri ?? '');

           foreach (array_keys($out[$fd->id]['facet_values']) as $fv_key) {
                
                $out[$fd->id]['facet_values'][$fv_key]['meta'] = $meta->facet_values->{$fv_key};

                // break down the provenance
                $new_provs = array();
                foreach ($out[$fd->id]['facet_values'][$fv_key]['provenance'] as $prov) {
                        //wfo-4000019729-s-37-ancestor
                        $matches = array();
                        preg_match('/^(wfo-[0-9]{10})-s-([0-9]+)-([a-z]+)$/', $prov, $matches);

                        $wfo = $matches[1];
                        $name_doc = $index->getDoc($wfo);

                        $source_id  = $matches[2];
                        $source_doc = $index->getDoc('wfo-fs-'. $source_id);

                        if($source_doc){
                            $source_doc = json_decode($source_doc->json_t);

                            $new_provs[] = array(
                                'wfo_id' => $wfo,
                                'full_name_html' => $name_doc->full_name_string_html_s,
                                'full_name_plain' => $name_doc->full_name_string_plain_s,
                                'source_id' => $source_id,
                                'source_name' => $source_doc->name,
                                'kind' => $matches[3],
                            );
                        }else{
                            error_log("No source document found with id " . 'wfo-fs-'. $source_id);
                        }

                }
                $out[$fd->id]['facet_values'][$fv_key]['provenance'] = $new_provs;
           
            }
           
        }


        return $out;

    }

    /*

    public static function indexScores(){

        global $mysqli;
        $index = new SolrIndex();

        // get the date of the last updated one in the index.
        $query = array(
            'query' => "kind_s:wfo-facet-value-score",
            "limit" => 1, 
            "sort" => "modified_dt desc",
            "fields" => array('modified_dt'), 
        );
        $docs = $index->getSolrDocs((object)$query);

        if(!$docs || count($docs) < 1){
            $modified_string = '1972-05-20T17:33:18Z';
        }else{
            $modified_string = $docs[0]->modified_dt;
        }

        $modified_date = new DateTime($modified_string);
        $modified_sql = $modified_date->format('Y-m-d H:i:s');

        echo "Indexing Scores\n";
       
        $sql = "SELECT * FROM `wfo_scores` WHERE `modified` > '{$modified_sql}' AND meta_json is not null;"; // no need to order as we are not paging
        $response = $mysqli->query($sql, MYSQLI_USE_RESULT); // we allow for big result set

        echo $response->num_rows . "\tscores to be indexed.\n";

        $page_counter = 0;

        $solr_docs = array();
        while($row = $response->fetch_assoc()){

            $solr_doc = array(
                'id' => "wfo-fvs-{$row['wfo_id']}-{$row['source_id']}-{$row['value_id']}",
                'kind_s' => 'wfo-facet-value-score',
                'wfo_id_s' => $row['wfo_id'],
                'source_id_s' => $row['source_id'],
                'value_id_s' => $row['value_id'],
                'modified_dt' => str_replace(' ', 'T', $row['modified']) . 'Z', // convert the date format
                'json_t' => $row['meta_json']
            );

            $solr_docs[] = $solr_doc;

            // save them in pages of 1,000
            if(count($solr_docs) > 1000){
                echo "Saving page ... ";
                $index->saveDocs($solr_docs);
                echo "saved.\n";
                $solr_docs = array();
            }

        }

        // any left over
        echo "Saving last page ... ";
        $index->saveDocs($solr_docs);
        echo " saved.\n";


        // work through all the ones in the scores tables that have changes since that data 
        // and add them in
        
        // need to include 

    }
    */
    /*
    public static function indexSnippets(){

        global $mysqli;
        $index = new SolrIndex();

        // get the date of the last updated one in the index.
        $query = array(
            'query' => "kind_s:wfo-snippet",
            "limit" => 1, 
            "sort" => "modified_dt desc",
            "fields" => array('modified_dt'), 
        );
        $docs = $index->getSolrDocs((object)$query);

        if(!$docs || count($docs) < 1){
            $modified_string = '1972-05-20T17:33:18Z';
        }else{
            $modified_string = $docs[0]->modified_dt;
        }

        $modified_date = new DateTime($modified_string);
        $modified_sql = $modified_date->format('Y-m-d H:i:s');

        echo "Indexing Snippets modified since $modified_sql\n";
       
        $sql = "SELECT * FROM `snippets` WHERE `modified` >= '{$modified_sql}' AND meta_json is not null;"; // no need to order as we are not paging
        echo $sql;
        $response = $mysqli->query($sql, MYSQLI_USE_RESULT); // we allow for big result set

        echo $response->num_rows . "\t snippets to be indexed.\n";

        $page_counter = 0;

        $solr_docs = array();
        while($row = $response->fetch_assoc()){

            $solr_doc = array(
                'id' => "wfo-snippet-{$row['id']}",
                'kind_s' => 'wfo-snippet',
                'wfo_id_s' => $row['wfo_id'],
                'source_id_s' => $row['source_id'],
                'modified_dt' => str_replace(' ', 'T', $row['modified']) . 'Z', // convert the date format
                'json_t' => $row['meta_json']
            );

            $solr_docs[] = $solr_doc;

            // save them in pages of 1,000
            if(count($solr_docs) > 1000){
                echo "Saving page";
                echo $page_counter++;
                echo " ... ";
                $index->saveDocs($solr_docs);
                echo "saved.\n";
                $solr_docs = array();
            }

        }

        // any left over
        echo "Saving last page ... ";
        $index->saveDocs($solr_docs);
        echo " saved.\n";


        // work through all the ones in the scores tables that have changes since that data 
        // and add them in
        
        // need to include 

    }
    */
}