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
    $local_file_dir = "../data/session_data/user_{$user['id']}";
    @mkdir($local_file_dir, 0777,true);

    $local_file_path = $store->downloadFile($local_file_dir);

    if($local_file_path){

        // we create an importer instance and put it in the session.
        if($snippet_mode){
            // we are importing snippets
            $importer = new ImporterSnippets($local_file_path, $store->file->oid, $source_id);
        }else{
            // we are importing facet data
            $importer = new ImporterFacets($local_file_path, $store->file->oid, $source_id, $facet_value['facet_value_id']);
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
