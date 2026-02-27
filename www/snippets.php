<?php
require_once('../include/language_codes.php');
require_once('header.php');

// build a list of the categories from the enumeration in the db
// we need this later
$result = $mysqli->query("SHOW COLUMNS FROM `sources` LIKE 'snippet_category'");
$row = $result->fetch_assoc();
$result->close();

$type = $row['Type'];
preg_match("/'(.*)'/i", $type, $matches);
$categories = explode(',', $matches[1]);
array_walk($categories, function(&$v){$v = str_replace("'", "", $v);});
sort($categories); // now in order
$categories_string = "'" . implode("','", $categories) . "'";

// if god they can create new snippet sources
if($user){
    echo '<div style="float: right;">';
    echo '&nbsp;<a 
        class="btn btn-sm btn-outline-secondary"
        href="source.php?facet_value_id=snippet"
        role="button"
        data-bs-toggle="tooltip"
        data-bs-placement="bottom"
        title="Create a new source of text snippets of kind and language." 
        >Create snippet source</a>';
    echo '</div>';
} // is god

?>
<p><a href="index.php">Fyllo</a> → Snippets</p>
<h1>Snippets</h1>
<p>Snippets of text describing taxa.</p>

<h2>Sources</h2>

<form action="snippets.php" method="GET">
<select name="category" onchange="this.form.submit()">
    <option value="">~ Any category ~</value>
<?php
    // create drop downs to choose filter 
    $sql = "SELECT `snippet_category`, count(*) as n FROM `sources` WHERE `snippet_category` IS NOT NULL";

    // if they have set a language and it is a good language
    if(@$_GET['language'] && isset($language_codes_alpha3[$_GET['language']])){
        $sql .= " AND `snippet_language` = '{$_GET['language']}'";
    } 

    $sql .= " GROUP BY `snippet_category` ORDER BY `snippet_category`";

    $response = $mysqli->query($sql);
    $rows = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();

    foreach($rows as $row){
        $cat_label = ucwords($row['snippet_category'] ?? '');
        $selected = @$_GET['category'] == $row['snippet_category']  ? 'selected' : '';
        echo "<option value=\"{$row['snippet_category']}\" {$selected}>{$cat_label} - {$row['n']}</option>";
    }

?>
</select>
&nbsp;
<select name="language" onchange="this.form.submit()">
    <option value="">~ Any language ~</value>
<?php
    // create drop downs to choose filter 
    $sql = "SELECT `snippet_language`, count(*) as n FROM `sources` WHERE `snippet_language` IS NOT NULL";

    // if they have set a category and it is a good language
    if(@$_GET['category'] && in_array($_GET['category'], $categories)){
        $sql .= " AND `snippet_category` = '{$_GET['category']}'";
    } 

    $sql .= " GROUP BY `snippet_language` ORDER BY `snippet_language`";

    $response = $mysqli->query($sql);
    $rows = $response->fetch_all(MYSQLI_ASSOC);
    $response->close();

    foreach($rows as $row){
        $lang_label = $language_codes_alpha3[$row['snippet_language']]['eng'];
        $selected = @$_GET['language'] == $row['snippet_language']  ? 'selected' : '';
        echo "<option value=\"{$row['snippet_language']}\" {$selected}>{$lang_label} - {$row['n']}</option>";
    }

?>
</select>

</form>

<?php

if(!@$_GET['category'] && !@$_GET['language']){
    echo "<hr/><p>Please select a category and/or a language to narrow down the choices a bit.</p><hr/>";
}else{
?>
<table class="table">

    <thead>
      <th scope="col">#</th>
      <th scope="col">Category</th>
      <th scope="col">Language</th>
      <th scope="col">Name</th>
      <th scope="col">Description</th>
    </thead>

<?php

        $sql = "SELECT * FROM sources as s ";

        $lang_code = @$_GET['language'] && isset($language_codes_alpha3[$_GET['language']]) ? $_GET['language'] : '';
        $lang_label = $lang_code ? $language_codes_alpha3[$lang_code]['eng'] : '';

        $cat_code = @$_GET['category'] && in_array($_GET['category'], $categories) ? $_GET['category'] : '';
        $cat_label = ucwords($cat_code);

        if($lang_code && $cat_code){
            $sql .= " WHERE `snippet_category` = '$cat_code' AND `snippet_language` = '$lang_code' ";
        }elseif($lang_code){
            $sql .= " WHERE `snippet_language` = '$lang_code' ";
        }elseif($cat_code){
            $sql .= " WHERE `snippet_category` = '$cat_code'  ";
        }

        // how to order?
        // get the languages as an alphabetical list to sort on
        $languages_string = "'" . implode("','", array_keys($language_codes_alpha3)) . "'";

        $sql .= " ORDER BY
                FIELD(`snippet_category`, {$categories_string}),
                FIELD(`snippet_language`, {$languages_string})    
            ;";

        $result = $mysqli->query($sql);

        while($row = $result->fetch_assoc()){

            echo "<tr>";

            echo "<th scope=\"row\"><a href=\"source.php?source_id={$row['id']}\">{$row['id']}</a></th>";


            $cat_label = ucwords($row['snippet_category']);
            echo "<td>{$cat_label}</td>";

            $lang_key = $row['snippet_language'] ? $row['snippet_language'] : 'eng';
            $lang_label = $language_codes_alpha3[$lang_key]['eng'];
            echo "<td>{$lang_label}</td>";
            
            echo "<td>{$row['name']}</td>";
            echo "<td>{$row['description']}</td>";
            echo "<tr/>";
        }

?>
</table>
<?php

} // end rendering the table of sources

require_once('footer.php');

?>