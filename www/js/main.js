const graphQlUri = "https://list.worldfloraonline.org/gql.php";
let listChanged = false; // a global handle for when to refresh the list


// search results are cached in local storage
// we need to make sure this doesn't overfill
// or go stale.

// not too many
if (localStorage.length > 50000) {
    alert("Over 50,000 items. Clearing local storage.");
    localStorage.clear();
}

// clear it after each data release (on the solstices)
$created_month = localStorage.getItem('created_month');
const d = new Date();
if (!$created_month) {
    localStorage.setItem('created_month', d.getMonth());
} else {
    created = parseInt($created_month);
    $now = d.getMonth();

    // if it was created in the first half of the year and we are in the second
    // or it was created in the second half and we are in the first
    // then reinitialize the storage.
    if ((created < 6 && $now > 5) || (created > 5 && $now < 6)) {
        alert("New data release. Clearing local storage.");
        localStorage.clear();
        localStorage.setItem('created_month', $now);
    }
}


function runGraphQuery(query, variables, giveBack) {

    const payload = {
        'query': query,
        'variables': variables
    }

    var options = {
        'method': 'POST',
        'contentType': 'application/json',
        'headers': {},
        'body': JSON.stringify(payload)
    };

    const response = fetch(graphQlUri, options)
        .then((response) => response.json())
        .then((data) => giveBack(data));

    return;
}


function replaceNameListItem(wfo) {

    // firstly see if we have the name in localstorage
    let name_json = localStorage.getItem(wfo);
    if (name_json) {
        let name = JSON.parse(name_json);
        let li = getNameListItem(name);
        let old_li = document.getElementById(wfo);
        old_li.replaceWith(li);
        return;
    }

    // fetch a name for the wfo from the index if
    // because we don't have it local
    let get_query =
        `query NameFetch($id: String!){
                    taxonNameById(nameId: $id){
                        id
                        stableUri
                        fullNameStringPlain
                        fullNameStringHtml
                        nomenclaturalStatus
                        role
                        rank
                        currentPreferredUsage {
                            pathString
                            hasName {
                                id
                                stableUri
                                fullNameStringHtml
                            }
                        }
                    }
                }`;

    runGraphQuery(get_query, {
        id: wfo
    }, (response) => {

        let name = response.data.taxonNameById;
        let li = getNameListItem(name);
        let old_li = document.getElementById(wfo);
        old_li.replaceWith(li);
        localStorage.setItem(wfo, JSON.stringify(name));

    });


}

/**
 * Returns a Dom Node of a list item
 * representing the name object
 * @param {Object} name 
 */
function getNameListItem(name) {

    const li = document.createElement("li");
    li.setAttribute('class', 'list-group-item');

    // plant details
    const col_left = document.createElement("div");
    li.appendChild(col_left);
    col_left.style.maxWidth = '80%';
    col_left.overflowX = 'hidden';

    const p = document.createElement("p");
    col_left.appendChild(p);

    const name_a = document.createElement("a");
    p.appendChild(name_a);
    name_a.innerHTML = name.fullNameStringHtml + "&nbsp;↗";
    name_a.setAttribute('href', name.stableUri);
    name_a.setAttribute('target', 'wfo');

    const span = document.createElement("span");
    p.appendChild(span);
    span.innerHTML = "&nbsp;(" + name.id + ")";

    const status_span = document.createElement("span");
    p.appendChild(status_span);
    status_span.innerHTML = `&nbsp;<strong>${name.nomenclaturalStatus} : ${name.role}</strong>`;

    // if it is an accepted name then we add a link to view the indexed value
    if (name.role == 'accepted')
        status_span.innerHTML = status_span.innerHTML + `&nbsp;:&nbsp;<a href="taxa.php?wfo_id=${name.id}">inspect</a>`;

    // add the accepted name if we have it
    if (name.currentPreferredUsage && name.id != name.currentPreferredUsage.hasName.id) {

        const strong = document.createElement("strong");
        p.appendChild(strong);
        strong.innerHTML = "&nbsp;of&nbsp;";

        const syn_a = document.createElement("a");
        p.appendChild(syn_a);
        syn_a.innerHTML = name.currentPreferredUsage.hasName.fullNameStringHtml + "&nbsp;↗";

        syn_a.setAttribute('href', name.currentPreferredUsage.hasName.stableUri);
        syn_a.setAttribute('target', 'wfo');

        const span = document.createElement("span");
        p.appendChild(span);
        span.innerHTML = `<strong>&nbsp;:&nbsp;</strong><a href="taxa.php?wfo_id=${name.currentPreferredUsage.hasName.id}">inspect</a>`;
    }

    // add the path in if we have it
    if (name.currentPreferredUsage && name.currentPreferredUsage.pathString) {
        p.appendChild(document.createElement("br"));
        p.appendChild(document.createTextNode(name.currentPreferredUsage.pathString));
    }

    return li;


}

function toggleListMembership(node, wfo, source_id, value_id) {

    node.innerHTML = "Updating...";
    fetch(`list_widget.php?wfo=${wfo}&source_id=${source_id}&value_id=${value_id}&toggle=true`)
        .then(x => x.text())
        .then(y => node.innerHTML = y);
    listChanged = true;

}

function callProgressBar(div, file_name) {
    fetch(file_name)
        .then((response) => response.json())
        .then((json) => {
            console.log(json.message);
            div.innerHTML = `<div class="alert alert-${json.level}" role="alert">${json.message}</div>`;
            if (!json.complete) callProgressBar(div, file_name);
            else window.location = 'source.php?tab=import&source_id=' + json.source_id;
        });
    listChanged = true;
}

// define the GraphQL query string ahead of times
const lookup_query =
    `query NameSearch($terms: String!){
                    taxonNameSuggestion(
                        termsString: $terms
                        limit: 100
                    ) {
                        id
                        stableUri
                        fullNameStringPlain
                        fullNameStringHtml
                        nomenclaturalStatus
                        role
                        rank
                        currentPreferredUsage {
                            pathString
                            hasName {
                                id
                                stableUri
                                fullNameStringHtml
                            }
                        }
                    }
                }`;

function nameLookup(e, nameList, sourceId, valueId) {

    let query_string = e.target.value.trim();
    if (query_string.length > 3) {

        // tell them we are looking
        nameList.innerHTML = "<li class=\"list-group-item\">Searching...</li>";

        // call the api
        runGraphQuery(lookup_query, {
            terms: query_string
        }, (response) => {

            //console.log(response.data);

            // remove the current children
            nameList.childNodes.forEach(child => {
                nameList.removeChild(child);
            });

            response.data.taxonNameSuggestion.forEach(name => {
                //console.log(name);
                nameList.appendChild(getNameListItem(name));
            });

            // if we haven't found anything then put a message in
            if (nameList.childNodes.length == 0) {
                nameList.innerHTML =
                    `<li class=\"list-group-item\">Nothing found for "${query_string}" </li>`;
            }
        });


    } else {
        nameList.innerHTML = "<li class=\"list-group-item\">Add 4 or more letters to search</li>";
    }
};