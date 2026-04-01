<?php

require_once('../include/FileStore.php');
require_once('../include/ImporterFacets.php');
require_once('../include/ImporterSnippets.php');

$store = new FileStore($source['file_path']);

if($store->file){
    $git_oid = $store->file->oid;
}else{
    $git_oid = 'n/a';
    echo "<div class=\"alert alert-danger\" role=\"alert\">Can't find the file in GitHub!</div>";
}
$local_oid = $source['oid'];

if($git_oid != $local_oid) $alert = 'style="color: red"';
else $alert = '';


if($user && $store->file){
    $disabled = '';
}else{
    $disabled = 'disabled';
}

// 

if(@$_POST['import_button']){
    
    //create a temporary dir we can work with
    $input_file_dir = "../data/session_data/user_{$user['id']}";
    @mkdir($input_file_dir, 0777,true);

    // what kind of remote file do we have?
    $remote_path = parse_url($store->file->downloadUrl, PHP_URL_PATH);
    $remote_filename = pathinfo($remote_path, PATHINFO_FILENAME);
    $remote_extension = pathinfo($remote_path, PATHINFO_EXTENSION);
    
    // download the file
    $input_file_path =  "{$input_file_dir}/{$remote_filename}.{$remote_extension}";

    // fetch it from github
    $curl = curl_init();
    curl_setopt($curl, CURLOPT_URL, $store->file->downloadUrl); // where the remote file is
    curl_setopt($curl, CURLOPT_USERAGENT, 'World Flora Online: Fyllo CMS'); // tell them who we are
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // yes we want to data
    $out = fopen($input_file_path, 'w'); // get a handle to write to
    curl_setopt($curl, CURLOPT_FILE, $out); // write it to the file

    // headers required to say we want the raw download and not a json summary.
    curl_setopt($curl, CURLOPT_HTTPHEADER, array(
        'Accept:application/vnd.github.v3.raw'
    ));

    // do it
    $result = curl_exec($curl);
    curl_close($curl);
    fclose($out);

    // double check we got it - and set flag to stop import if we can't uncompress it
    if(file_exists($input_file_path)) $have_file = true;
    else $have_file = false;

    // we have downloaded the file do we need to unzip it?
    if($remote_extension == 'zip' && $have_file){
        
        $zip = new ZipArchive();
        
        // only one file so we can get its name
        if ($zip->open($input_file_path) == TRUE) {

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
                echo '<div class="alert alert-danger" role="alert"><strong>The zip archive contains multiple files ('. $file_list .'). Which is right? Correct in GitHub.</strong></div>';
                // clean up
                unlink($input_file_path);

            }else{
            
                // extract the file
                $filename = $filenames[0];
                $zip->extractTo($input_file_dir, $filename);

                // remove the zip file
                unlink($input_file_path);

                // make the extracted file into the new input file
                $input_file_path = "{$input_file_dir}/{$filename}";
            
            } // only one file in zip

        }else{
            $have_file = false; // don't have a good file
            // tell the user
                echo '<div class="alert alert-danger" role="alert"><strong>Something is corrupt with the zip file.</strong></div>';
            // clean up
            unlink($input_file_path);
        }
        
    }

    if($have_file){

        // we create an importer instance and put it in the session.
        if($snippet_mode){
            // we are importing snippets
            $importer = new ImporterSnippets($input_file_path, $store->file->oid, $source_id);
        }else{
            // we are importing facet data
            $importer = new ImporterFacets($input_file_path, $store->file->oid, $source_id, $facet_value['facet_value_id']);
        }
        
        $_SESSION['importer'] = serialize($importer);

        // we will then render the progress bar that will 
        // repeatedly call the importer in the session
        $render_import_progress  = true;
    }else{
        $render_import_progress  = false;
    }
}else{
    $render_import_progress  = false;
}

?>

<p class="lead">Here you can import data from the source file into Fyllo. 
    The OIDs are hashes of the files used by GitHub to detect changes. 
    If they are different then the GitHub file has changed since the last import.
    If they are the same then there is no need to import the file again, unless you know the Fyllo data is corrupted.
</p>

<div class="mb-3">
    <table>
        <tr>
            <th style="text-align: right;">File path: </th>
            <td><?php echo $source['file_path'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;">GitHub File: </th>
            <td><a href="https://github.com/worldflora/wfo-text-content/blob/main/<?php echo $source['file_path'] ?>" target="github" >View on GitHub</a></td>
        </tr>
        <tr>
            <th style="text-align: right;">GitHub OID: </th>
            <td <?php echo $alert ?> ><?php echo $git_oid ?></td>
        </tr>
        <tr>
            <th style="text-align: right;">Last import OID: </th>
            <td <?php echo $alert ?> ><?php echo $local_oid ?></td>
        </tr>
        <tr>
            <th style="text-align: right;">Last import date: </th>
            <td><?php echo $source['last_import'] ?></td>
        </tr>
    </table>
</div>
<form method="POST" action="source.php">
    <input type="hidden" name="source_id" value="<?php echo $source_id ?>" />
    <input type="hidden" name="tab" value="import" />
    <div class="mb-3">
        <button type="submit" name="import_button" value="import" class="btn btn-primary" <?php echo $disabled ?>>Import now</button>
    </div>
</form>

<?php
    if($render_import_progress){
?>
<div id="import_progress_bar">
    <div class="alert alert-warning" role="alert"><strong>Downloading ... </strong></div>
</div>
<div>
    <a href="source.php?tab=import&source_id=<?php echo $source_id ?>">Cancel</a>
</div>
<script>
// call the progress bar every second till it is complete
const harvest_div = document.getElementById('import_progress_bar');
callProgressBar(harvest_div, 'source_import_progress.php');
</script>

<?php
    } // render the harvester bar
?>
