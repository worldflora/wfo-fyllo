<?php

require_once('../config.php');

// this will dump all the descriptions in emonocot in the right
// format to be uploaded

// check that the dump directory exists

$dump_dir = '../data/emonocot/';

// check the director exists
@mkdir($dump_dir, 0777, true);


$sql = "SELECT 
	t.identifier,
	d.`description`,
	d.`type`,
    d.`language`,
    t.scientificName, 
    d.taxon_id as emonocot_taxon_id,
    o.identifier as authority,
    o.commentsEmailedTo as comments_email,
    d.authority_id 
FROM emonocot.`description` as d
JOIN emonocot.`taxon` as t on d.taxon_id = t.id
JOIN emonocot.organisation as o on d.authority_id = o.id
order by authority_id, d.`type`, d.`language`;";


$response = $mysqli->query($sql, MYSQLI_USE_RESULT); // incremental fetch

$current_authority = null;
$current_type = null;
$current_language = null;
$out = null;
while($row = $response->fetch_assoc()){

    // we change files if we change authority or type
    if(
        $current_authority != $row['authority_id']
        ||
        $current_type != $row['type']
        ||
        $current_language != $row['language']
        ){
            
            $current_authority = $row['authority_id'];
            $current_type = $row['type'];
            $current_language = $row['language'];

            if($out) fclose($out);
            $file_path = "{$dump_dir}{$current_authority}_{$current_type}_{$current_language}.csv";
            echo "$file_path\n";

            $out = fopen($file_path, 'w');

            $header = array(
                'wfo_id',
                'description',
                'type',
                'language',
                'scientificName',
                'emonocot_taxon_id',
                'authority',
                'comments_email',
                'authority_id'
            );
            fputcsv($out, $header, escape: "\\");

        }

        fputcsv($out, $row, escape: "\\");

}

if($out) fclose($out);

echo "All done!\n";


