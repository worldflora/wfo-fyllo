
<?php

require_once('../include/language_codes.php');
require_once('header.php');

    $source_id = (int)@$_REQUEST['source_id'];

    $response = $mysqli->query("SELECT *, s.modified as source_modified, ss.modified as language_kind_modified FROM snippet_sources as ss join sources as s on ss.source_id = s.id WHERE s.id = $source_id;");
    $rows = $response->fetch_all(MYSQLI_ASSOC);

    if(!$rows){
        echo "<p>Error: No snippet source with id $source_id</p>";
    }

    $source = $rows[0];

    // if god they edit the snippet
    if($user){
        echo '<div style="float: right;">';
              
        // duplicate button - very useful!
        // we just create a hidden form with the values in and post them to the create script
        echo '<form style="display: inline;" method="POST" action="snippet_source_create.php">';
        echo '<input type="hidden" id="name" name="name" value="Copy - '. $source['name'] .'" />';
        echo '<input type="hidden" id="category" name="category" value="'.  $source['category'] .'" />';
        echo '<input type="hidden" id="language" name="language" value="'.  $source['language'] .'" />';
        echo '<input type="hidden" id="link_uri" name="link_uri" value="'.  $source['link_uri'] .'" />';
        echo '<input type="hidden" id="description" name="description" value="'.  $source['description'] .'" />';

        echo "&nbsp;<button 
            class=\"btn btn-sm btn-outline-secondary\"
            data-bs-toggle=\"tooltip\"
            data-bs-placement=\"bottom\"
            title=\"Make a duplicate source, perhaps for a different category.\" 
            >Duplicate</button>";

        echo '</form>';

        echo '</div>';
    } // is god


    echo '<p>&lt;- <a href="snippets.php">Snippet sources</a></p>';
    echo "<h1>{$source['name']}</h1>";
    echo "<p>{$source['description']}</p>";

?>

<ul class="nav nav-tabs" id="myTab" role="tablist" style="margin-bottom: 1em;">

    <!-- summary -->
    <li class="nav-item" role="presentation">
        <button 
            class="nav-link  <?php echo @$_REQUEST['tab'] ? '' : 'active' ?>"
            id="summary-tab"
            data-bs-toggle="tab"
            data-bs-target="#summary"
            type="button"
            role="tab">Summary</button>
    </li>
    <?php
     if($user){
?>

    <!-- Properties edit -->
    <li class="nav-item" role="presentation">
        <button 
            class="nav-link <?php echo @$_REQUEST['tab'] == "properties-tab" ? 'active' : '' ?>"
            id="properties-tab"
            data-bs-toggle="tab"
            data-bs-target="#properties"
            type="button"
            
            role="tab">Properties</button>
    </li>

    <!-- Upload -->
    <li class="nav-item" role="presentation">
        <button 
            class="nav-link <?php echo @$_REQUEST['tab'] == "upload-tab" ? 'active' : '' ?>"
            id="upload-tab"
            data-bs-toggle="tab"
            data-bs-target="#upload"
            type="button"
            role="tab">Upload</button>
    </li>

    <?php
     } // can edit
?>

</ul>

<div class="tab-content" id="myTabContent">

    <!-- Summary of the data in this source -->
    <div 
        class="tab-pane fade <?php echo @$_REQUEST['tab'] ? '' : 'show active' ?>" 
        id="summary" 
        role="tabpanel" 
        aria-labelledby="summary-tab">
    <p class="lead">
        Snippet source summary
    </p>
    <table class="table table-striped">
    <tbody>
        <tr>
            <th style="text-align: right; width: 33%;" scope="row">ID</th>
            <td><?php echo $source['source_id'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Category</th>
            <td><?php echo $source['category'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Language</th>
            <td><?php echo $source['language'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Created</th>
            <td><?php echo $source['created'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Title or description modified</th>
            <td><?php echo $source['source_modified'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Language or kind modified</th>
            <td><?php echo $source['language_kind_modified'] ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Last import</th>
            <td><?php echo $source['harvest_last'] ?></td>
        </tr>
        <?php
            $response = $mysqli->query("SELECT count(*) as n, count(distinct(`wfo_id`)) as 'distinct',  min(length(body)) as min, avg(length(body)) as average, max(length(body)) as max FROM wfo_facets.snippets where source_id = {$source['source_id']};");
            $rows = $response->fetch_all(MYSQLI_ASSOC);
            $response->close();
            $stats = $rows[0];
        ?>

        <tr>
            <th style="text-align: right;" scope="row">Number of snippets</th>
            <td><?php echo number_format($stats['n'], 0) ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Number of WFO IDs</th>
            <td><?php echo number_format($stats['distinct'], 0) ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Minimum length</th>
            <td><?php echo number_format($stats['min'], 0) ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Average length</th>
            <td><?php echo number_format($stats['average'], 0) ?></td>
        </tr>
        <tr>
            <th style="text-align: right;" scope="row">Maximum length</th>
            <td><?php echo number_format($stats['max'], 0) ?></td>
        </tr>

    </tbody>
    </table>
    </div>
    <?php
     if($user){
?>


    <!-- PROPERTIES -->
    <div 
        class="tab-pane fade <?php echo @$_REQUEST['tab'] == "properties-tab" ? 'show active' : '' ?>" 
        id="properties" 
        role="tabpanel"
    >
        <?php require_once('snippet_source_properties.php'); ?>
    </div>


    <!-- UPLOAD -->
    <div 
        class="tab-pane fade <?php echo @$_REQUEST['tab'] == "upload-tab" ? 'show active' : '' ?>"
        id="upload"
        role="tabpanel">
        <?php require_once('snippet_source_upload.php'); ?>
    </div>

    <?php
     } // can edit
?>

</div>



<?php
require_once('footer.php');
?>
