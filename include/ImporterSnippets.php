<?php

class ImporterSnippets{


    public string $filePath;
    public int $sourceId;
    public string $offset;
    public ?int $created = null;
    public $file = null;
    public $header = null;
    public string $oid;
    
    public function __construct($input_file_path, $oid, $source_id, $offset = 0){

        global $mysqli;

        $this->created = time();
        
        $this->filePath = $input_file_path;
        $this->sourceId = (int)$source_id;
        $this->oid = $oid;
        $this->offset = $offset;

        $this->file = fopen($this->filePath, 'r');

        // we always replace everything
        $mysqli->query("DELETE FROM snippets WHERE source_id = $this->sourceId and id > 0;");

        $this->seek($this->offset);

    }
        
    public function __sleep(){
        fclose($this->file);
        return array('filePath', 'sourceId', 'oid', 'offset', 'created', 'header');
    }
    
    public function __wakeup(){
        $this->file = fopen($this->filePath, 'r');
        $this->seek($this->offset);
    }

    public function seek($line){
        rewind($this->file);
        for ($i=0; $i < $line; $i++) {
            fgetcsv($this->file, escape: "\\");
        }
    }

    public function import($page_size){

        global $mysqli;

        for ($i=0; $i < $page_size; $i++) { 

            $row = fgetcsv($this->file, escape: "\\");

            // capture the header if there is one
            if($this->offset == 0){

                if(preg_match('/^wfo-[0-9]{10}$/', $row[0])){
                    // we have a wfo-id in the first column so we know this isn't a header row
                    // make one up
                    $this->header = array();
                    for($j = 0; $j < count($row); $j++){
                        $this->header[] = 'col_' . $j;
                    }
                }else{
                    // the first row is the header
                    $this->header = $row;
                    // let it carry on and up the offset etc - will be kicked out later
                }

            }

            $this->offset++;

            if(!$row){
                $mysqli->query("UPDATE sources SET last_import = now(), `oid` = '$this->oid' WHERE id = {$this->sourceId};");
                unlink($this->filePath);
                return $i;
            } 

            // wfo id is in first column
            $wfo_id = $row[0];
            
            // must be correct format
            if(!preg_match('/^wfo-[0-9]{10}$/', $wfo_id)) continue;

            // must exist in the cache - will be added if it isn't there
            if(NameCache::cacheName($wfo_id)){
                $safe_body =  $mysqli->real_escape_string($row[1]);

                $meta = array();
                for($j = 0; $j < count($this->header); $j++){
                    $meta[$this->header[$j]] = $row[$j];
                }

                $meta_json = json_encode((object)$meta);
                $meta_json_safe = $mysqli->real_escape_string($meta_json);
                $mysqli->query("INSERT INTO snippets (`wfo_id`, `source_id`, `body`, `meta_json`) VALUES ('$wfo_id', {$this->sourceId}, '{$safe_body}', '{$meta_json_safe}');");
                if($mysqli->error) error_log($mysqli->error);

            }

        }

        return $page_size;
        
    }
}