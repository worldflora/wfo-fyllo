<?php

require_once('../config.php');

/*
    Class that enables picking of files from a data store
    It is initially implemented as a mechanism to access a github repo
    but could be replaced with some other file store

*/

class FileStore{

    private ?String $githubUrl; // the URL to access the git hub repo
    private ?String $githubAccessToken; // needed to call github from a uri
    private ?String $path; // the path from the root of the git repo
    private ?String $error = null;
    public ?Array $entries = null;
    public ?Object $file = null;

    public function __construct($path = null){

        // pull in the variable we need from the config file.
        global $github_owner;
        global $github_repo;
        global $github_path; 
        global $github_graphql_endpoint;
        global $github_access_token;

        // if we have been constructed without a path then 
        // set the root in the config file
        if($path) $this->path = $path;
        else $this->path = $github_path;


        // build the graphQL query
        $query = "query {
  repository(owner: \"$github_owner\", name: \"$github_repo\") {
    object(expression: \"HEAD:{$this->path}\" ) {
      ... on Tree {
        entries {
          oid
          name
          type
          path
          }
      }
      ... on Blob {
        oid
        byteSize
        isBinary    
      }
    }
  }
}";

        $query_object = (object)array("query" => $query);
        $query_json = json_encode($query_object);

        $headers = array();
        $headers[] = 'Content-Type: application/json';
        $headers[] = 'Authorization: Bearer '.$github_access_token;

        $curl = curl_init($github_graphql_endpoint);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS');
        curl_setopt($curl, CURLOPT_POSTFIELDS, $query_json);
        curl_setopt($curl, CURLOPT_POST, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);

        $json = curl_exec($curl);
        $result = json_decode($json);

        if (curl_errno($curl)) {
            $this->error = curl_error($curl);
        }else{
            $this->error = null;

            // we have either passed a path to a file object (Blob)
            // or a directory (Tree)
            if(isset($result->data->repository->object->entries)){
                // we have entries we are a tree
                $this->entries = $result->data->repository->object->entries;

                // sort them nicely
                usort($this->entries, function($a, $b){ 
                    // directories (trees) sort before files
                    if($a->type == 'tree' && $b->type == 'blob') return -1;
                    if($a->type == 'blob' && $b->type == 'tree') return 1;
                    if($a->name > $b->name) return 1;
                    if($a->name < $b->name) return -1;
                    return 0;
                });

                // flag the files that are pickable
                foreach ($this->entries as $entry) {
                    if($entry->type == 'blob'){
                        // we can only pick csv and zip file - maybe had gz later?
                        $info = pathinfo($entry->path);
                        if( in_array($info['extension'], array('csv', 'zip')) ){
                            $entry->pickable = true;
                        }else{
                            $entry->pickable = false;
                        }
                    }
                }

                // put a link to the parent dir if there is one
                if($this->path != $github_path){
                    array_unshift($this->entries, (object)array(
                            'name' => '..',
                            'type' => 'tree',
                            'oid' => null,
                            'path' => dirname($this->path) . '/'
                        ));       
                }
                

            }else{
                // we don't have entries we are a file
                $this->file = $result->data->repository->object;
                if($this->file){
                    $this->file->downloadUrl = "https://api.github.com/repos/worldflora/wfo-text-content/contents/" . $this->path;
                }
            }

        }

    } // end constructor

    public function downloadFile($local_file_dir){
        
        // what kind of remote file do we have?
        $remote_path = parse_url($this->file->downloadUrl, PHP_URL_PATH);
        $remote_filename = pathinfo($remote_path, PATHINFO_FILENAME);
        $remote_extension = pathinfo($remote_path, PATHINFO_EXTENSION);

        // download the file
        $local_file_path =  "{$local_file_dir}/{$remote_filename}.{$remote_extension}";

        // curl doesn't like spaces
        $download_url = str_replace(' ', '%20', $this->file->downloadUrl);

        // fetch it from github
        $curl = curl_init($download_url); // where the remote file is
        curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS'); // tell them who we are
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // yes we want to data
        $out = fopen($local_file_path, 'w'); // get a handle to write to
        curl_setopt($curl, CURLOPT_FILE, $out); // write it to the file

        // headers required to say we want the raw download and not a json summary.
        curl_setopt($curl, CURLOPT_HTTPHEADER, array(
            'Accept:application/vnd.github.v3.raw'
        ));

        // do it
        $result = curl_exec($curl);

        if(curl_error($curl)) echo curl_error($curl);

        curl_close($curl);
        fclose($out);

        // double check we got it - and set flag to stop import if we can't uncompress it
        if(file_exists($local_file_path)) $have_file = true;
        else $have_file = false;

         // we have downloaded the file do we need to unzip it?
        if($remote_extension == 'zip' && $have_file){
        
            $zip = new ZipArchive();
            
            // only one file so we can get its name
            if ($zip->open($local_file_path) == TRUE) {

                // we can't simply count the files because mac put extra
                // files in zips starting with _
                $filenames = array();
                for ($i = 0; $i < $zip->numFiles; $i++) {
                    $n = $zip->getNameIndex($i);
                    if(!preg_match('/^_/', $n)) $filenames[] = $n;
                }

                if(count($filenames) != 1){

                    $have_file = false; // don't have a good file
                    // tell the user
                    $file_list = implode(', ', $filenames);
                    // clean up
                    unlink($local_file_path);

                }else{
                
                    // extract the file
                    $filename = $filenames[0];
                    $zip->extractTo($local_file_dir, $filename);

                    // remove the zip file
                    unlink($local_file_path);

                    // make the extracted file into the new input file
                    $local_file_path = "{$local_file_dir}/{$filename}";
                
                } // only one file in zip

            }else{
                $have_file = false; // don't have a good file
                unlink($local_file_path);
            }
        
        }

        if($have_file){
            return $local_file_path;
        }else{
            return null;
        }



    }


}

// for testing this can be run on the command line
if (php_sapi_name() === 'cli'){
    echo "Testing FileStore\n";

    if(count($argv) > 1){
        $path = $argv[1];
    }else{
        $path = null;
    }

    $store = new FileStore($path);

    print_r($store->entries);
    print_r($store->file);


}

?>