<?php

/*

This used to generate the download files for a data source.

It is designed to be serialized into the session between paging calls
in a similar way the ImporterFacets class is.

*/

class ExporterFacets{


    public int $sourceId;
    public object $source;
    public object $facetValue;
    public int $offset = 0;
    public int $depth = 0;
    public bool $inSynonyms = false;
    public ?int $created = null;
    public bool $finished = false;
    public ?string $sqlitePath = null;
    public ?int $total = 0;
    public string $phase = 'sqlite'; // we work through a phase to create a sqlite db then we do a phase for each flavour of download file
    public ?string $htmlFilePath = null; 
    public ?string $csvFilePath = null; 
    public ?string $title = null;
    public bool $includeSynonyms = false;


    private $db = null;


    public function __construct($source_id, $include_syns = false){

        global $mysqli;

        $this->created = time();
        $this->sourceId = $source_id;
        $this->includeSynonyms = $include_syns;

        // set up the locations to put the files
        @mkdir(WFO_EXPORTS_DIRECTORY, 0777, true);

        $this->sqlitePath = WFO_EXPORTS_DIRECTORY . $this->sourceId . '.sqlite';

        // we destroy it to stop it growing!
        @unlink($this->sqlitePath);

        $this->db = new SQLite3($this->sqlitePath, SQLITE3_OPEN_CREATE | SQLITE3_OPEN_READWRITE);

        $this->db->query('CREATE TABLE IF NOT EXISTS "records" (
            "wfo_id" TEXT,
            "name" TEXT,
            "role" TEXT,
            "rank" TEXT,
            "parent_id" TEXT,
            "path" TEXT,
            "featured" INT,
            "body" TEXT,
            UNIQUE("wfo_id")
        )');

        $response = $mysqli->query("SELECT count(*) as n FROM wfo_scores WHERE source_id = $source_id;");
        $rows = $response->fetch_all(MYSQLI_ASSOC);
        $this->total = $rows[0]['n'];
        $response->close();

        // set up the files for the output
        $response = $mysqli->query("SELECT * FROM `sources` as s JOIN `facet_value_sources` as fvs on s.id = fvs.source_id WHERE id = $source_id");
        $source = $response->fetch_assoc();
        $response->close();

        $downloadsDirectory = 'downloads/' . $source_id . '/';
        @mkdir($downloadsDirectory, 0777, true);

        $this->title = $source['name'];

        $file_name = preg_replace('/[^A-Za-z0-9]/', '_', $source['name']);
        $this->htmlFilePath = $downloadsDirectory . $file_name . '.html';
        $this->csvFilePath = $downloadsDirectory . $file_name . '.csv';

        // clear out those files because we are starting again
        @unlink($this->htmlFilePath);
        @unlink($this->csvFilePath);


    }

    /**
     * When this is serialized to the session
     * we close the files but keep a handle on 
     * some of the variables 
     */
    public function __sleep(){
        $this->db->close();
        // the fields we perist
        return array('sourceId', 'source', 'facetValue', 'offset', 'depth', 'total', 'phase', 'title', 'inSynonyms', 'csvFilePath', 'htmlFilePath', 'created', 'sqlitePath', 'finished');
    }
    
    /**
     * When we unserialise we re-open the files 
     * to append more data to them
     */
    public function __wakeup(){
        $this->db = new SQLite3($this->sqlitePath, SQLITE3_OPEN_CREATE | SQLITE3_OPEN_READWRITE);
    }

    public function finished(){
        return $this->finished;
    }

    public function getMessage(){

        $total_pretty = number_format($this->total, 0);
        $offset_pretty = number_format($this->offset, 0);
        $percent = round(($this->offset/$this->total)*100);
        if($percent > 100) $percent = 100;

        switch ($this->phase) {
            case 'sqlite':
                $title = "Creating local data cache.";
                break;
            case 'html':
                $title = "Creating HTML file.";
                break;
            case 'csv':
                $title = "Creating csv file.";
                break;    
            default:
                $title = "Working ...";
                break;
        }

        $out = '<div style="width: 100%;">';
        $out .= "<span>&nbsp;{$title}</span>";
        $out .= '</div>';

        $out .= '<div style="width: 100%; border: solid black 1px; background-color: gray;">';
        $out .= '<div style="width: '. $percent .'%; border: none; background-color: blue; color: white;">';
        $out .= "<span>&nbsp;</span><span style=\"float: right;\">{$percent}%&nbsp;</span>";
        $out .= '</div>';
        $out .= '</div>';

        $out .= '<div style="width: 100%;">';
        $out .= "<span>&nbsp;{$offset_pretty}</span>";
        $out .= "<span style=\"float: right;\">{$total_pretty}&nbsp;</span>";
        $out .= '</div>';
        
        return $out; 
    }



    /**
     * Actually process the next page
     */
    public function page(){

        if($this->phase == 'sqlite') return $this->pageSqlite();
        if($this->phase == 'html') return $this->pageHtml();
        if($this->phase == 'csv') return $this->pageCsv();
        $this->finished = true;
        $this->deleteSqliteDb();

    }

    public function pageSqlite(){

        global $mysqli;
        $index = new SolrIndex();
        $path = array();

        $response = $mysqli->query("SELECT wfo_id FROM wfo_scores where source_id = $this->sourceId ORDER BY wfo_id LIMIT 50 OFFSET $this->offset;");

        $this->db->exec('BEGIN');
        while($row = $response->fetch_assoc()){

            // we need to build a tree of docs even if this is a synonym - unlike what we do during indexing.
            $target = $index->getDoc($row['wfo_id']);
            // if we haven't got the target by the doc id maybe it is a deduplicated wfo_id?
            if(!$target){

                $solr_query = array(
                    'query' => "wfo_id_deduplicated_ss:{$row['wfo_id']}",
                    'filter' => array("classification_id_s:" . WFO_DEFAULT_VERSION)
                );
                $solr_response = $index->getSolrResponse($solr_query);
                if(isset($solr_response->response->docs) && $solr_response->response->docs){
                    $target = $solr_response->response->docs[0];
                }

            }

            if(!$target) continue; // do nothing if we didn't find anything
            $path[] = $target; // add it to the ones to be processed.

            // get all the records with this path 
            // if it has a path (not unplaced or deprecated)
            if(isset($target->name_ancestor_path)){
                $query = array(
                    'query' => "name_ancestor_path:{$target->name_ancestor_path}", // everything in this tree of names
                    "limit" => 10000, // big limit - not run out of memory theoretically could fail on stupid numbers of synonyms
                    'filter' => array("classification_id_s:{$target->classification_id_s}"// filtered by this classification
                ) );
                $docs = $index->getSolrDocs((object)$query);
                $all = array();
                foreach($docs as $doc){
                    $all[$doc->id] = $doc;
                }
                
                // if the target is a synonym then we start there
                if(isset($target->accepted_id_s)){
                    $path[] = $all[$target->accepted_id_s];
                }

                while(true){

                    $last = end($path);

                    // if the last one has a parent and the parent is in the list of all then we add it
                    if(isset($last->parent_id_s) && $last->parent_id_s && isset($all[$last->parent_id_s]) ){
                        $path[] = $all[$last->parent_id_s];
                    }else{
                        // no parent found so get out of here
                        break;
                    }

                }

                if($this->includeSynonyms){
                    // we add synonyms of the target (or its parent if it is a synonym)
                    if(isset($target->accepted_id_s)){
                        $syns_of = $all[$target->accepted_id_s];
                    }else{
                        $syns_of = $target;
                    }
                    foreach($all as $syn){
                        if(isset($syn->accepted_id_s) && $syn->accepted_id_s == $syns_of->id ) $path[] = $syn;
                    }
                }


            }
 
            $statement = $this->db->prepare('INSERT OR IGNORE INTO "records" (
                    "wfo_id",
                    "name",
                    "role",
                    "rank",
                    "parent_id",
                    "path",
                    "featured",
                    "body"
                )VALUES(
                    :wfo_id,
                    :name,
                    :role,
                    :rank,
                    :parent_id,
                    :path,
                    :featured,
                    :body
                )');


            // add all the parents
            $target_added = false;
            foreach($path as $p){

                $statement->bindValue(':wfo_id', $p->wfo_id_s);
                $statement->bindValue(':name', $p->full_name_string_plain_s);
                $statement->bindValue(':role', $p->role_s);
                $statement->bindValue(':rank', $p->rank_s);

                // we use parent_id for both syns and accepted relationships
                if($p->role_s == 'accepted') $statement->bindValue(':parent_id', isset($p->parent_id_s) ? substr($p->parent_id_s, 0, 14) : null ); // strip the qualifier
                if($p->role_s == 'synonym') $statement->bindValue(':parent_id', isset($p->accepted_id_s) ? substr($p->accepted_id_s, 0, 14) : null ); // strip the qualifier
               
                $statement->bindValue(':path', isset($p->name_path_s) ? $p->name_path_s : null);
                $statement->bindValue(':featured', $p->wfo_id_s == $target->wfo_id_s ? 1 : 0);
                $statement->bindValue(':body', json_encode($p));
                $statement->execute();

            } 


        }// mysql row
        $this->db->exec('COMMIT');

        if($response->num_rows == 0){
           //$this->finished = true;
           $this->phase = 'html'; 
           $this->offset = 0;
        }else{
            $this->offset += 50;
        }
        
        error_log('Page: ' . $this->offset);
    }


    /**
     * Do a page of HTML exporting
     * - not really a page as we are doing 
     * - a widthwise crawl of the tree
     */
    public function pageHtml(){

        // we only want to start when we get to the common ancestor
        // not at the code root
        $common_ancestor_id = $this->getCommonAncestorId();

        // does the output file exist?
        if(!file_exists($this->htmlFilePath)){
            // open it and insert the header stuff
            $out = fopen($this->htmlFilePath, 'w');
            
            // add a bom for UTF-8 encoding
            fwrite($out, "\xEF\xBB\xBF");

            $this->writeHtmlHeader($out);

            // check we are starting at the beginning of the db
            $this->offset = 0;
            $this->total = $this->db->querySingle("SELECT count(*) from records;");

        }else{
            // just open it for append
            $out = fopen($this->htmlFilePath, 'a');
        }

        // sort by the name path and then role. 
        // synonyms have the same name path as their accepted names so they 
        // will come after the accepted name 
        $response = $this->db->query("SELECT * FROM `records` where role in ('accepted', 'synonym') order by `path`, `role`, `name` limit 1000 offset {$this->offset} ");
        $row_count = 0;
        $reached_common_ancestor = false;
        while ($row = $response->fetchArray()) {

            $row_count++;

            if($this->offset == 0 && !$reached_common_ancestor){
                if($row['wfo_id'] !=  $common_ancestor_id){
                    continue;
                }else{
                    $reached_common_ancestor = true;
                    $this->depth = substr_count($row['path'], '/');
                } 
            }

            // how deep are we?
            $new_depth = substr_count($row['path'], '/');
            if($new_depth > $this->depth){
                fwrite($out, str_repeat('<li><ul>',  $new_depth - $this->depth ));
            };
            if($new_depth < $this->depth){
                fwrite($out, str_repeat('</ul></li>',  $this->depth - $new_depth ));
            };
            $this->depth = $new_depth;

            // we are starting a run of synonyms
            if($row['role'] == 'synonym' && !$this->inSynonyms){
                $this->inSynonyms = true;
                fwrite($out, '<li><strong>syns:</strong><ul>');
            }

            // we are past the end of the synonyms
            if($row['role'] == 'accepted' && $this->inSynonyms){
                $this->inSynonyms = false;
                fwrite($out, '</ul></li>');
            }

            $this->writeTaxonName($out, $row, !$this->inSynonyms);

            // hold the taxon path so we know if we need to do synonyms or not
            $this->lastTaxonPath = $row['path'];


        }

        if($row_count == 0){

            // close off the last lists
            fwrite($out, str_repeat('</ul></li>',  $this->depth));

            $this->writeUnplaced($out);
            $this->writeDeprecated($out);
            $this->writeHtmlFooter($out);
            fclose($out);

            @unlink($this->htmlFilePath . '.zip');
            
            $zip = new ZipArchive;
            $zip->open($this->htmlFilePath . '.zip', ZIPARCHIVE::CREATE);
            $zip->addFile($this->htmlFilePath, basename($this->htmlFilePath));
            $zip->close();

            @unlink($this->htmlFilePath);
            
            $this->finished = false;
            $this->offset = 0;
            $this->phase = 'csv';


        }else{
            $this->offset += 1000;
        }
        
    }

    private function writeUnplaced($out){

        fwrite($out, "<h2>Unplaced Names</h2>");
        fwrite($out, "<p>Names that have not been placed in the classification by a WFO taxonomist yet.</p>");
        $response = $this->db->query("SELECT * FROM `records` where role = 'unplaced' order by `name`;");
        fwrite($out, '<ul>');
        $row_count = 0;
        while ($row = $response->fetchArray()) {
            $this->writeTaxonName($out, $row, false);
            $row_count++;
        }
        if($row_count == 0) fwrite($out, '<li>None</li>');
        fwrite($out, '</ul>');
        $response->finalize();
    }

    private function writeDeprecated($out){

        fwrite($out, "<h2>Deprecated Names</h2>");
        fwrite($out, "<p>Names that can't be placed because they were created in error or represent an unused rank.</p>");
        
        $response = $this->db->query("SELECT * FROM `records` where role = 'deprecated' order by `name`;");
        fwrite($out, '<ul>');
        $row_count = 0;
        while ($row = $response->fetchArray()) {
            $this->writeTaxonName($out, $row, false);
            $row_count++;
        }
        if($row_count == 0) fwrite($out, '<li>None</li>');
        fwrite($out, '</ul>');
        $response->finalize();

    }

    private function writeTaxonName($out, $row, $abbreviate_genus = true){

            // get the full data for the record
            $json = json_decode($row['body']);

            $class = $row['featured'] ? 'featured' : 'not-featured';

            fwrite($out, '<li>');
        
            $display_name = $json->full_name_string_html_s;

            // replace the genus name if there is one (we are below genus level)
            if(isset($json->genus_string_s) && $abbreviate_genus){
                $display_name = str_replace($json->genus_string_s, substr($json->genus_string_s, 0, 1) . '.', $display_name);      
            }
        
            fwrite($out,  "<strong class=\"{$class}\">$display_name</strong>");
            fwrite($out,  "&nbsp;[{$json->rank_s}]&nbsp;");
            fwrite($out,  @$json->citation_micro_s);
            fwrite($out,  "&nbsp;<a href=\"https://list.worldfloraonline.org/{$json->wfo_id_s}\" target=\"wfo_list\">{$json->wfo_id_s}</a>");
            fwrite($out,  "&nbsp;[<a href=\"https://list.worldfloraonline.org/rhakhis/ui/index.html#{$json->wfo_id_s}\" target=\"rhakhis\">Rhakhis</a>]");

            fwrite($out, '</li>');

    }

    public function pageCsv(){

        // does the output file exist?
        if(!file_exists($this->csvFilePath)){

            // open it and insert the header stuff
            $out = fopen($this->csvFilePath, 'w');
            
            // add a bom for UTF-8 encoding
            fwrite($out, "\xEF\xBB\xBF");
            
            // put a header row in
            fputcsv($out, array(
                'wfo_id',
                'scientific_name',
                'taxonomic_status',
                'named_in_list',
                'rank',
                'parent_id',
                'accepted_id',
                'name_path',
                'name_no_authors',
                'authors',
                'micro_citation',
                'nomenclatural_status'
            ), escape: "\\");

            // check we are starting at the beginning of the db
            $this->offset = 0;
            $this->total = $this->db->querySingle("SELECT count(*) from records;");

        }else{
            // just open it for append
            $out = fopen($this->csvFilePath, 'a');
        }

        $response = $this->db->query("SELECT * FROM `records` order by `path` NULLS LAST, `role`, `name` LIMIT 1000 offset {$this->offset} ");
        $row_count = 0;
        while ($row = $response->fetchArray()) {

            $row_count++;

            $csv_row = array();
            $csv_row[] = $row['wfo_id'];
            $csv_row[] = $row['name'];
            $csv_row[] = $row['role'];
            $csv_row[] = $row['featured'];
            $csv_row[] = $row['rank'];

            if($row['parent_id']){
                if($row['role'] == 'accepted'){
                    $csv_row[] = $row['parent_id'];
                    $csv_row[] = null;
                }else{
                    $csv_row[] = null;
                    $csv_row[] = $row['parent_id'];
                }
            }else{
                $csv_row[] = null;
                $csv_row[] = null;
            }

            $csv_row[] = $row['path'];

            // now some fluff from the rest of the record
            $json = json_decode($row['body']);

            $csv_row[] = @$json->full_name_string_no_authors_plain_s;
            $csv_row[] = @$json->authors_string_s;
            $csv_row[] = @$json->citation_micro_t;
            $csv_row[] = @$json->nomenclatural_status_s;

            fputcsv($out,$csv_row, escape: "\\");

        }

        if($row_count == 0){

            // close of the last lists
            fclose($out);
            
            @unlink($this->csvFilePath . '.zip');

            $zip = new ZipArchive;
            $zip->open($this->csvFilePath . '.zip', ZIPARCHIVE::CREATE);
            $zip->addFile($this->csvFilePath, basename($this->csvFilePath));
            $zip->close();

            @unlink($this->csvFilePath);

            $this->finished = true;
            $this->offset = 0;
            $this->phase = 'done';

        }else{
            $this->offset += 1000;
        }
    }

    /**
     * we need to clear up the database because
     * the file can be quite large.
     */
    public function deleteSqliteDb(){
        if($this->db) $this->db->close();
        @unlink($this->sqlitePath);
    }

    public function getCommonAncestorId($wfo_id = null){

        // we don't have taxon id to work with
        // so we start at the root of all
        if(!$wfo_id){
            $wfo_id = $this->db->querySingle("select wfo_id from records where `rank` = 'code' and parent_id is null;");
        }

        $number_kids = $this->db->querySingle("SELECT count(*) FROM `records` WHERE `parent_id` = '$wfo_id';");
        if($number_kids > 1){
            return $wfo_id;
        }else{
            $child_wfo = $this->db->querySingle("SELECT wfo_id FROM `records` WHERE `parent_id` = '$wfo_id';");
            return $this->getCommonAncestorId($child_wfo);
        }
    
    
    }

    private function writeHtmlHeader($out){

        fwrite($out, '
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>WFO List Download</title>
    <style>
        body{
            font-family: Arial, Helvetica, sans-serif;
        }
        .featured{
            color: green;
        }
        .wfo-name-authors{
            color: gray;
        }
        ul {
            list-style-type: none;
        }
    </style>
  </head>
  <body>
  <h1>' . $this->title . '</h1>
  <p>Exported ' . date("F d Y @ H:i:s") . '. Names highlighted in <span class="featured">green</span> are mentioned in the list, other names give their context within the current classification.</p>
  <h2>Classification</h2>
  <ul>
  ' );

    }


    private function writeHtmlFooter($out){

        fwrite($out, '
        </ul>
        </body>
</html>
' );

    }

} // class