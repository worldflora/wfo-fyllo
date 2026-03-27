<?php

require_once('../config.php');
require_once('../include/SolrIndex.php');
require_once('../include/WfoFacets.php');
require_once('../include/ImporterFacets.php');
require_once('../include/NameCache.php');

// this will run the harvester on sources that need it.

$response = $mysqli->query("SELECT * 
	FROM sources as s 
    JOIN facet_value_sources as fvs on s.id = fvs.source_id
    WHERE (s.harvest_frequency != 'never' AND length(s.harvest_uri) > 0)
    AND
    (
        (s.harvest_frequency = 'monthly' AND s.harvest_last <= CURRENT_DATE() - INTERVAL 1 MONTH)
        OR
        (s.harvest_frequency = 'weekly' AND s.harvest_last <= CURRENT_DATE() - INTERVAL 1 WEEK)
        OR
        (s.harvest_frequency = 'daily' AND s.harvest_last <= CURRENT_DATE() - INTERVAL 1 DAY)
        OR
        s.harvest_last is null
    )
    ORDER BY s.harvest_last;");

$sources = $response->fetch_all(MYSQLI_ASSOC);
$response->close();

foreach($sources as $source){

    echo "Processing\t{$source['id']}\t{$source['name']}\n";
    $now = time();
    $input_file_path = "../data/session_data/harvester/source_{$source['id']}";
    @mkdir($input_file_path, 0777,true);
    
    $input_file_path .= "/$now.csv";
    
    if(file_put_contents($input_file_path, file_get_contents(trim($source['harvest_uri'])))){

        $importer = new ImporterFacets($input_file_path, $source['harvest_overwrites'] == 1 ? true : false, $source['id'], $source['facet_value_id']);

        $page_size = 100;
        $count = 0;
        while($imported = $importer->import($page_size)){
            $count += $imported;
            echo "\tImported: " . number_format($count, 0) . "\n";
            if($imported < $page_size) break;
        }

        echo "\tFinished\n";

    }else{
        echo "Something went wrong writing the file\n";
        echo "Data url: {$source['harvest_uri']} \n";
        echo "File path: {$input_file_path} \n";
    }

}

echo "\nFinished all\n";