<?php

require_once('../config.php');

echo "Extract facets from WCVP and put them in a CSV file\n";

echo "Loading ISO to Q mappings\n";

$in = fopen('../data/iso_countries/wiki_countries.csv', 'r');
$header = fgetcsv($in);
$iso2q = array();
while($line = fgetcsv($in)){
    $iso2q[$line[3]] = $line[1];
}
fclose($in);
echo "\t loaded ". count($iso2q) . " mappings\n";

echo "Loading TDWG L3 to ISO mappings\n";

$in = fopen('../data/tdwg_geography/tdwg_level3_to_iso_alpha2.csv', 'r');
$header = fgetcsv($in);
$tdwg2iso = array();
while($line = fgetcsv($in)){
    $tdwg2iso[$line[0]] = $line[1];
}
fclose($in);

echo "\t loaded ". count($tdwg2iso) . " mappings\n";

echo "Working through WCVP file\n";

$in = fopen("/Users/rogerhyam/Downloads/wcvp/wcvp_distribution.csv", 'r');
$header = fgetcsv($in, 0, '|');

$out = fopen('../data/iso_countries/wcvp_countries_Q47542613.csv', 'w');
fputcsv($out, array('wfo_id', 'q_number'), escape: "\\");

$count = 0;
while($line = fgetcsv($in, 0, '|')){

    $plant_id = $line[1];
    $tdwg_l3 = $line[6];

    if(isset($tdwg2iso[$tdwg_l3])){
        $iso = $tdwg2iso[$tdwg_l3];
        if(isset($iso2q[$iso])){
            $q = $iso2q[$iso];

            $response = $mysqli->query("SELECT wfo_id, taxon_rank FROM kew.wcvp WHERE plant_name_id = $plant_id");
            if($response->num_rows){
                $row = $response->fetch_assoc();
                if(strtolower($row['taxon_rank']) != 'species') continue;
                if(preg_match('/^wfo-[0-9]{10}$/', $row['wfo_id'])){
                    $pair = array($row['wfo_id'], $q);
                    fputcsv($out, $pair, escape: "\\");
                    echo "{$count}\t{$pair[0]}\t{$pair[1]}\n";
                }
            }

            
        }
    }

    $count++;

    // debug
//    if($count > 100) break;
}

fclose($out);
fclose($in);