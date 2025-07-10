<?php

require_once('../config.php');

echo "Extract life forms from wcvp database table to CSV file.\n";

$map = get_mapping();

// add a file to write it to.
$out = fopen('../data/life_forms/wcvp_life_forms_Q47542613.csv', 'w');
fputcsv($out, array('wfo_id', 'q_number'), escape: "\\");

$count = 0;
foreach($map as $word => $q){

    // get a list of rows containing the word
    $word_safe = $mysqli->real_escape_string($word);
    $response = $mysqli->query("SELECT wfo_id
        FROM kew.wcvp 
        WHERE wfo_id is not null
        AND lifeform_description 
        like '%$word_safe%'");
    $rows = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();

    
    // for each word we add a score.
    foreach ($rows as $row) {
        if(preg_match('/^wfo-[0-9]{10}$/', $row['wfo_id'])){
            $pair = array($row['wfo_id'], $q);
            fputcsv($out, $pair, escape: "\\");
            echo "{$count}\t$word\t{$pair[0]}\t{$pair[1]}\n";
            $count++;
        }
    }
    
    // debug
    //if($count > 100) break;
}

function get_mapping(){

    return array(
        "annual" => "Q192691",
        "biennial" => "Q189774",
        "shrub" => "Q42295",
        "subshrub" => "Q12867502",
        "succulent" => "Q189939",
        "tree" => "Q10884",
        "epiphyte" => "Q188238",
        "epiphytic" => "Q188238",
        "liana" => "Q14079",
        
        // aquatics
        "hydroannual" => "Q186101",
        "hydrogeophyte" => "Q186101",
        "hydroperennial" => "Q186101",
        "hydrophyte" => "Q186101",
        "hydroshrub" => "Q186101",
        "hydrosubshrub" => "Q186101",
        "semiaquatic" => "Q186101",
        "semiaquatic" => "Q186101"
    );

    return array(
        "annual" => "Q192691",
        "bamboo" => "Q670887",
        "biennial" => "Q189774",
        "climber" => "Q917284",
        "climbing" => "Q917284",
        "epiphyte" => "Q188238",
        "epiphytic" => "Q188238",
        "geophyte" => "Q3092468",
        "helophyte" => "Q1592155",
        "hemiepiphyte" => "Q188238",
        "hemiepiphytic" => "Q188238",
        "hemiepiphyte" => "Q4371101",
        "hemiepiphytic" => "Q4371101",
        "hemiparasite" => "Q20739318",
        "hemiparasitic" => "Q20739318",
        "herbaceous" => "Q190903",
        "holoparasite" => "Q127498",
        "holoparasitic" => "Q127498",
        "hydroannual" => "Q186101",
        "hydrogeophyte" => "Q186101",
        "hydroperennial" => "Q186101",
        "hydrophyte" => "Q186101",
        "hydroshrub" => "Q186101",
        "hydrosubshrub" => "Q186101",
        "hydroannual" => "Q192691",
        "hydrogeophyte" => "Q3092468",
        "hydroperennial" => "Q157957",
        "hydroshrub" => "Q42295",
        "hydrosubshrub" => "Q12867502",
        "liana" => "Q14079",
        "lithophyte" => "Q1321691",
        "lithophytic" => "Q1321691",
        "monocarpic" => "Q354608",
        "parasitic" => "Q127498",
        "perennial" => "Q157957",
        "semiaquatic" => "Q186101",
        "semiaquatic" => "Q186101",
        "semisucculent" => "Q189939",
        "semisucculent" => "Q189939",
        "shrub" => "Q42295",
        "subshrub" => "Q12867502",
        "succulent" => "Q189939",
        "tree" => "Q10884"
    );

}