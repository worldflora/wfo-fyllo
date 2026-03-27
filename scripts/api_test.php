<?php

require_once('../config.php');

// do a curl call to the api to 

// get the wfo from the command line and moan if there isn't one
if(count($argv) < 2){
    echo "\nYou must pass either:\n";
    echo "\t- the wfo id of the taxon to render\n";
    echo "\t- 'facets' for metadata about facets\n";
    echo "\t- 'sources' for metadata about sources\n";
    echo "\t- 'offset 0' to test the modified names call\n";
    exit;
}

$verb = trim($argv[1]);

if(preg_match('/^wfo-[0-9]{10}$/', $verb)){
    test_taxon_data($verb);
}elseif($verb == 'sources' || $verb == 'facets'){
   test_metadata($verb);
}else{
    test_modified($argv[2]);
}

function test_metadata($verb){

    global $api_bearer_token;

    $headers = array();
    $headers[] = 'Content-Type: application/json';
    $headers[] = 'Authorization: Bearer '. $api_bearer_token;
    $curl = curl_init('http://localhost:3030/api.php?metadata=' . $verb);
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS');
    curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
    $json = curl_exec($curl);
    $result = json_decode($json);
    print_r($result);
}

function test_modified($offset){

    global $api_bearer_token;

    $headers = array();
    $headers[] = 'Content-Type: application/json';
    $headers[] = 'Authorization: Bearer '. $api_bearer_token;
    $curl = curl_init('http://localhost:3030/api.php?offset=' . $offset);
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS');
    curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
    $json = curl_exec($curl);
    $result = json_decode($json);
    
    if(json_last_error() != JSON_ERROR_NONE){
        echo json_last_error_msg();
        echo $json;
    }else{
        print_r($result);
    }

}

function test_taxon_data($wfo_id){

    global $api_bearer_token;

    // get the graph of the taxon
    $taxon_graph = get_taxon_graph($wfo_id);
    $taxon_graphs = array($taxon_graph); // it expects an array of graphs

    // post it to the script
    $graph_json = json_encode($taxon_graphs);

    $headers = array();
    $headers[] = 'Content-Type: application/json';
    $headers[] = 'Authorization: Bearer '. $api_bearer_token;

    $curl = curl_init('http://localhost:3030/api.php');
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS');
    curl_setopt($curl, CURLOPT_POSTFIELDS, $graph_json);
    curl_setopt($curl, CURLOPT_POST, 1);
    curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);

    $json = curl_exec($curl);

    $result = json_decode($json);

    //echo $json; exit;

    if(json_last_error() != JSON_ERROR_NONE){
        echo json_last_error_msg();
        echo $json;
    }else{
        print_r($result);
    }

}




// get the taxon tree by calling the graphql
function get_taxon_graph($wfo_id){

    $query = "query{
            taxonNameById(nameId: \"{$wfo_id}\") {
                id
                currentPreferredUsage{
                    classificationId
                    id
                    hasName{
                        id
                        wfoIdsDeduplicated
                    }
                    hasSynonym{
                        id
                        wfoIdsDeduplicated
                    }
                    path{
                        hasName{
                            id
                            wfoIdsDeduplicated
                        }
                    }
                }
            }
        }";

        $query_object = (object)array("query" => $query);
        $query_json = json_encode($query_object);

        $headers = array();
        $headers[] = 'Content-Type: application/json';

        $curl = curl_init(PLANT_LIST_GRAPHQL_URI);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS');
        curl_setopt($curl, CURLOPT_POSTFIELDS, $query_json);
        curl_setopt($curl, CURLOPT_POST, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);

        $json = curl_exec($curl);
        $result = json_decode($json);

        // chop it down to the bit we are interested in
        $result = $result->data->taxonNameById;

        if(!isset($result->currentPreferredUsage->hasName)){
            echo "$wfo_id is unplaced so can't render";
            exit;
        }

        $out = array();
        $out['classification'] = $result->currentPreferredUsage->classificationId;
        $out['taxon'] = array_merge(array($result->currentPreferredUsage->hasName->id), $result->currentPreferredUsage->hasName->wfoIdsDeduplicated);

        // the path
        $out['path'] = array();
        foreach (array_reverse($result->currentPreferredUsage->path) as $anc) {
            if($anc->hasName->id == $wfo_id) continue;
            $out['path'][] = array_merge(array($anc->hasName->id), $anc->hasName->wfoIdsDeduplicated) ;
        }

        // the synonyms
        $out['synonyms'] = array();
        foreach ($result->currentPreferredUsage->hasSynonym as $syn) {
            $out['synonyms'][] = array_merge(array($syn->id), $syn->wfoIdsDeduplicated);
        }
        
        return (object)$out;

}

        

