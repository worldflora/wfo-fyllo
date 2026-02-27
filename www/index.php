<?php
    require_once('header.php');
?>

<h1>Fyllo: Text and facet data management for the World Flora Online</h1>

<p class="lead">
    Faceted searching is very common in information retrieval systems such as internet shopping sites.
    It enables the user to restrict their search to items that have a particular feature or set of features - a
    certain colour or size for example.
    It would be very useful to subset the list of over 400,000 recognized taxa in the WFO Plant List in a
    similar way - perhaps by country, life form or threat status.
</p>
<p>
    This service provides a way to tag the accepted taxa in the WFO Plant List with useful attributes for searching and 
    information retrieval.
    It handles data from multiple sources that binds specific attributes to WFO <strong>name</strong> IDs and then calculates the
    values for accepted <strong>taxa</strong> and inserts those into the plant list index in such a way that the provenance of the data
    isn't lost and can be displayed to the end user. 
</p>
<p>
    In addition to facet values it is possible to associate blocks of text (snippets) with taxa via their names.
    This works in a similar way to facet indexing. A data source provides a list of snippets in a particular language
    and for a particular category (e.g. morphology). Each snippet is associated with a WFO ID

</p>
<h2>Definitions</h2>
<dl>
    <dt>Facet</dt>
    <dd>Some feature of a plant, the equivalent of a character<sup>*</sup> in plant identification e.g. "Life Form".
    </dd>
    <dt>Facet Value</dt>
    <dd>A form that a facet takes, the equivalent of a character state e.g. "Tree" for lifeform.</dd>
    <dt>Snippet</dt>
    <dd>A piece of text that is in a particular language and category and associate with a WFO name ID.</dd>
    <dt>Source</dt>
    <dd>The service does not contain original information but collates it from multiple sources.
        Each source is a simple list of WFO IDs mapped to a facet value. The source also specifies the provenance
        for the list. Note that provenance is per list not per individual ID mapping. Each source only contains
        information about a single facet values.
    </dd>

</dl>
<h2>Separation of concerns</h2>
<p>
    This service does not store any information about nomenclature or taxonomy but only the WFO IDs of names.
    It calls the WFO Plant List API for the latest version of the data.
</p>

<p style="text-align: center;">
<img src="images/faceting_publish_process.png" alt="Diagram of publishing process" style="border: solid 1px gray;" />
</p>

<h2>Facets vs characters</h2>
<p>
    We use facet and facet value in preference to character and character state because there is
    no intention to build an identification system. This is purely about information retrieval and presentation.)
</p>

<p style="text-align: center;">
<img src="images/facets_vs_characters.png" alt="Diagram of publishing process" style="border: solid 1px gray;" />
</p>

<h2>Data sources and facets</h2>

<p>
    Each data source is for one facet value or text catetory only.
</p>

<p style="text-align: center;">
<img src="images/facets_values_datasources.png" alt="Diagram of publishing process" style="border: solid 1px gray;" />
</p>

<p>
More details of implemenation can be found on the <a href="https://github.com/rogerhyam/wfo-facets">README page</a> of the GitHub repository.
</p>

<?php
    require_once('footer.php');
?>