<?php
    require_once('header.php');
?>

<h1>Fyllo: Text and facet data management for the World Flora Online</h1>

<p class="lead">
    This application is used to manage text and facet based data that has been contributed for inclusion in 
    the World Flora Online. 
</p>
<h2>Separation of concerns</h2>
<p>
    Fyllo <strong>DOES NOT</strong> store any information about nomenclature or taxonomy but only the WFO IDs of names.
    It calls the WFO Plant List API for the latest version of the data to display in the interface but is 
    agnostic about whether a name is accepted or a synonym or even how it is spelt.
</p>
<p>
    Fyllo <strong>IS NOT</strong> the authoritative source for the text and facet data. It loads all this data from the 
    github <a href="https://github.com/worldflora/wfo-text-content">WFO Text Content</a> repository</a>.
    Data must be committed to this repository as a CSV file and then imported into Fyllo where it
    is available to be indexed against the latest WFO classification.
    Data in the repository can be worked on collaboratively and versioned appropriately before import.
</p>
<p> 
    Fyllo <strong>IS</strong> authoritative for the metadata about the data imported. It stores the 
    descriptions of the data sources and the language and category tags for each imported file.
    This data is not stored in the github repository.
</p>
<p>
    Fyllo <strong>DOES NOT</strong> index the data but it provides an <a href="api.php">API</a> that a separate indexing application
    can use to request the data for taxon in return for providing the taxonomic placements of the associated names in a 
    taxon graph.
</p>
<h2>Snippets</h2>
<p>
    Snippets are short pieces of text associated with a WFO name ID. They are stored in a CSV file in the GitHub repository.
    Each CSV file represents a <strong>Data Source</strong>. When the CSV is imported into Fyllo it is associated
    with a category (e.g. "distribution" or "vernacular") and a language (using ISO 3 letter codes).
    The first column contains valid WFO IDs of names. The second column contains the actual text of the snippet.
    Subsequent columns are considered <strong>row metadata</strong> (see below).
    The first row should contain column headers.
    Rows that don't have a valid WFO ID in the first column are ignored.
</p>
<h2>Facets</h2>
<p>
    Faceted searching is very common in information retrieval systems such as internet shopping sites.
    It enables the user to filter their search results to items that have a particular feature or set of features - a
    certain colour or size for example. In WFO it is very useful to subset the list of over 400,000 recognized 
    taxa in the WFO Plant List in a similar way - perhaps by country, life form or threat status.
</p>
<dl>
    <dt>Facet</dt>
    <dd>Some feature of a plant, the equivalent of a character in plant identification e.g. "Life Form".
    </dd>
    <dt>Facet Value</dt>
    <dd>A form that a facet takes, the equivalent of a character state e.g. "Tree" for lifeform.</dd>
</dl>
<p>
    Facets and their possible Facet Values are managed in Fyllo. Each Facet Value can have one or more <strong>Data Sources</strong>
    that are CSV files in the GitHub repository just like those for Snippets. Data Sources for Facet Values are even simpler
    than those for Snippets in that they only require the first column to contain a valid WFO ID.
    All subsequent columns are treated as row metadata. A Data Source can only contain data about a single Facet Value.
    You can't, for example, have a CSV file containing a list of different lifeforms. Such files would need to be split 
    into multiple Data Source files using a script during ingest to GitHub. 
</p>

<h2>Provenence and row metadata</h2>
<p>
    The World Flora Online is a synoptic work. It presents an overview of existing data, not data of its own. 
    It is therefore important that each peice of data presented must be linked back to its source. 
    Data provenance is all important. Fyllo tracks data provenance at two levels.
</p>
<h3>Data source level provenance</h3>
<p>
    Each Data Source (a file in GitHub that has been imported into Fyllo) has a name, description and link stored in 
    Fyllo. This is entered manually when the data source is set up and can be updated at any time. It ensures that
    all data has provenance at least at this level. 
</p>
<p>
    An example of a data source that might only have provenance at this level would be data extracted from a book
    where the metadata links back to that publication, perhaps in BHL.
</p>
<h3>Row level metadata</h3>
<p>
    A design goal to ingest as wider a range of data from as many different sources as possible. 
    Different domains of data and different data sources will have a wide range of requirements for what metadata 
    can or should be associated with each data point.
    Fyllo therefore uses a very flexible approach to row level metadata. 
    Any additional columns in the CSV file are converted into name-value pairs using the column heading as the 
    name. These are propagated into the indexing system as part of the provenance and can be displayed to the user on request.
    It is also possible to extend their functionality at display or analysis time if needed.
    Using this method it is possible to give specific attribution or license information or onward links to 
    further data. If needed the system could be used to represent existing metadata standards in future
    but doesn't require all data providers to both agree on and supply their data in a specific
    form prior to getting the system working.
</p>

<h2>Note on facets vs characters</h2>
<p>
    We use facet and facet value in preference to character and character state because there is
    no intention to build an identification system. Firstly this is purely about information retrieval and presentation.
    Secondly some of the facets, like conservation status, are not characteristics of a taxon that could be used
    in building a classification or for identification at all.
</p>

<p>
More details of implemenation can be found on the <a href="https://github.com/rogerhyam/wfo-facets">README page</a> of the GitHub repository.
</p>

<?php
    require_once('footer.php');
?>