<?php

require_once('../include/ImporterSnippets.php');


// the upload on the source page
$render_upload_progress = false;
if($_POST && isset($_FILES["input_file"]) && $user && $_FILES["input_file"]["type"] == 'text/csv'){

    // we save the file by the user id and source.
    $now = time();
    $input_file_path = "../data/session_data/user_{$user['id']}/source_{$source_id}";
    @mkdir($input_file_path, 0777,true);
    $input_file_path .= "/$now.csv";
    move_uploaded_file($_FILES["input_file"]["tmp_name"], $input_file_path);

    // load it all in the session because we will run through 
    // these things in ajax calls.
    $importer = new ImporterSnippets($input_file_path, $source_id);
    $_SESSION['importer'] = serialize($importer);
    $render_upload_progress = true;

}else{
    unset($_SESSION['importer']);
    $importer = false;
}

?>
<p class="lead">
    Upload snippets
</p>
<p>
    Use this form to upload the text snippets for this data source.
    We don't expect text data to change because it should have been extracted from a static, published resource. 
    Therefore, you can't edit textual data directly in the facet server but will need to edit it
    locally and re-upload it if you need to change somegthing.
</p>
<p>The format for uploading files is very simple.
    The file should be a CSV file with the <strong>first column</strong> containing ten digit WFO IDs (e.g. wfo-0000878441).
    The <strong>second column</strong> should contain the text snippet.
    All <strong>other columns</strong> will be imported as unparsed metadata.
    Any rows that don't start with a valid WFO ID will be ignored.
    If you don't have WFO IDs for your names yet you can add them to the CSV using the <a
        href="https://list.worldfloraonline.org/matching.php">name matching tool available
        on the WFO Plant List API</a>.
    You will need to edit the output of the matching tool so that the text snippets column comes directly 
    after the WFO ID column that is inserted by the tool.
</p>
<p>
    Uploading data here always replaces all the existing data.
</p>

<?php

    if($user){
        if($render_upload_progress){
?>
<div id="upload_progress_bar">
    <div class="alert alert-warning" role="alert"><strong>Uploading ... </strong></div>
</div>
<div>
    <a href="snippet_source.php?tab=upload-tab&source_id=<?php echo $source_id ?>">Cancel</a>
</div>
<script>
// call the progress bar every second till it is complete
const upload_div = document.getElementById('upload_progress_bar');
callProgressBar(upload_div, 'snippet_source_upload_progress.php');
</script>

<?php
        }else{
?>

<form method="POST" action="snippet_source.php" enctype="multipart/form-data">
    <input type="hidden" name="tab" value="upload-tab" />
    <input type="hidden" name="source_id" value="<?php echo $source_id ?>" />
    <div class="mb-3">
        <label for="input_file" class="form-label">Select a file for upload.</label>
        <input class="form-control" type="file" id="input_file" name="input_file"
            onchange="if(this.value) document.getElementById('upload_button').disabled = false;">
    </div>
    <button type="submit" disabled="true" class="btn btn-primary" id="upload_button">Upload</button>
</form>

<?php
        }// rendering form
    }else{ // can use form
?>
<div class="alert alert-danger" role="alert">Sorry, you don't have rights to upload to this list.</div>
<?php
    } // can't edit form
?>