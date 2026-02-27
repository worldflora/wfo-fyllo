<?php

require_once('../config.php');

$response = $mysqli->query('SELECT * FROM kew_geo.iso_mapping WHERE tdwg_closest is null;');
echo $mysqli->error;
$iso_codes = $response->fetch_all(MYSQLI_ASSOC);
$response->close();

foreach ($iso_codes as $iso_code){

    $response = $mysqli->query("SELECT * FROM kew_geo.tree WHERE iso_code = '{$iso_code['alpha_2']}';");
    echo $mysqli->error;
    $nodes = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();

    // entirely missing?
    if(count($nodes) == 0){
        $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = 'MISSING' WHERE id = {$iso_code['id']};");
        echo $mysqli->error;
        continue;
    }

    // is there only one a this level?
    if(count($nodes) == 1){

        // is this the only one at level4 if so we use the parent.
        $response = $mysqli->query("SELECT * FROM kew_geo.tree WHERE parent_id = '{$nodes[0]['parent_id']}';");
        echo $mysqli->error;
        $siblings = $response->fetch_all(MYSQLI_ASSOC);
        $response->close();

        if(count($siblings) == 1){
            // I'm monotypic so use the parent one
            $response = $mysqli->query("SELECT * FROM kew_geo.tree WHERE id = '{$nodes[0]['parent_id']}';");
            echo $mysqli->error;
            $parents = $response->fetch_all(MYSQLI_ASSOC);
            $response->close();
            $parent = $parents[0];
            $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = '{$parent['tdwg_code']}' WHERE id = {$iso_code['id']};");
            echo $mysqli->error;
        }else{
            $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = '{$nodes[0]['tdwg_code']}' WHERE id = {$iso_code['id']};");
            echo $mysqli->error;
        }
        continue;
    }

    // there is more than one of them
    // do they have the same parent?
    $single_parent = true;
    $parent_id = $nodes[0]['parent_id'];
    foreach($nodes as $node){
        if($parent_id != $node['parent_id']){
            // alarm bells
            echo "\nCountry {$iso_code['alpha_2']} split across parents!\n";
            $single_parent = false;
            break;
        }
    }

    // we only have one parent.
    // does the parent contain multiple countries?
    if($single_parent){

        $response = $mysqli->query("SELECT distinct('iso_code') FROM kew_geo.tree WHERE parent_id = '{$nodes[0]['parent_id']}';");
        echo $mysqli->error;
        $siblings_iso_codes = $response->fetch_all(MYSQLI_ASSOC);
        $response->close();

        if(count($siblings_iso_codes) > 1){
            $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = 'AMBIGUOUS SIBLINGS' WHERE id = {$iso_code['id']};");
            continue;
        }else{
            // only one parent and no ambiguous siblings so parent is the one!
            $response = $mysqli->query("SELECT * FROM kew_geo.tree WHERE id = '{$nodes[0]['parent_id']}';");
            echo $mysqli->error;
            $parents = $response->fetch_all(MYSQLI_ASSOC);
            $response->close();
            $parent = $parents[0];
            $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = '{$parent['tdwg_code']}' WHERE id = {$iso_code['id']};");
            echo $mysqli->error;

        }

    }

    // There are multiple parents do we go up a level?
    // do the parents' children all have the same iso_code?
    $parents_ids = array();
    foreach($nodes as $node){
        $parents_ids[] = $node['parent_id'];
    }
    
    $parents_ids = implode(',', $parents_ids);
    $response = $mysqli->query("SELECT distinct(iso_code) FROM kew_geo.tree WHERE parent_id in ($parents_ids);");
    echo $mysqli->error;
    $descendants_codes = $response->fetch_all(MYSQLI_ASSOC);
    
    if(count($descendants_codes) == 1){
        // the descendants all have the same ISO so we can add it to all 
        $response = $mysqli->query("SELECT * FROM kew_geo.tree WHERE id in ($parents_ids);");
        echo $mysqli->error;
        $parents = $response->fetch_all(MYSQLI_ASSOC);
        $response->close();
        $tdwg_codes = array();
        foreach($parents as $p){
            $tdwg_codes[] = $p['tdwg_code'];
        }
        $tdwg_codes = implode(',', $tdwg_codes);           
        $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = '$tdwg_codes' WHERE id = {$iso_code['id']};");
        echo $mysqli->error;

    }else{

        // the descenants have different codes so we have to add
        // all the nodes
        $tdwg_codes = array();
        foreach($nodes as $node){
            $tdwg_codes[] = $node['tdwg_code'];
        }
        $tdwg_codes = implode(',', $tdwg_codes);           
        $mysqli->query("UPDATE kew_geo.iso_mapping SET tdwg_closest = '$tdwg_codes' WHERE id = {$iso_code['id']};");
         echo $mysqli->error;

    }


}