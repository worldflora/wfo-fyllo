<?php

 /**
 * A wrapper around the name cache table that
 */
class NameCache{

    public static function cacheName($wfo_id){

        global $mysqli;

        // is it present in the cache
        $response = $mysqli->query("SELECT * FROM name_cache WHERE wfo_id = '$wfo_id';");
        if($response->num_rows > 0){
            return true;
        }else{
            // we don't have it so we need to call to get it
            $graph_ql_query = '
                query NameFetch($id: String!){
                    taxonNameById(nameId: $id){
                        fullNameStringPlain
                        wfoIdsDeduplicated
                    }
                }
            ';


            $graph_ql_variables = (object)array('id' => $wfo_id);

            $payload = (object) array(
                'query' => $graph_ql_query,
                'variables' => $graph_ql_variables
            );

            $ch = curl_init( PLANT_LIST_GRAPHQL_URI );
            # Setup request to send json via POST.
            curl_setopt( $ch, CURLOPT_POSTFIELDS, json_encode($payload) );
            curl_setopt( $ch, CURLOPT_HTTPHEADER, array('Content-Type:application/json'));
            # Return response instead of printing.
            curl_setopt( $ch, CURLOPT_RETURNTRANSFER, true );
            # Send request.
            $result = curl_exec($ch);
            curl_close($ch);

            //error_log(print_r(curl_error($ch), true));

            $result = json_decode($result);

            if(isset($result->data->taxonNameById)){
                $name = $result->data->taxonNameById->fullNameStringPlain;
                $name_safe = $mysqli->real_escape_string($name);

                // we have to be careful with deduplication IDs 
                // they may have started with a duplicate ID and the name would have 
                // been returned with the real one plus the passed one 
                // or a name may just have deduplication IDs
                $all_wfo_ids = $result->data->taxonNameById->wfoIdsDeduplicated; // might be empty but we don't care
                $all_wfo_ids[] = $wfo_id;

                foreach($all_wfo_ids as $wfo_id){
                    // we might get duplicate keys this way because of the deduplication thing.
                    $mysqli->query("INSERT INTO name_cache (`wfo_id`, `name`) VALUES ('$wfo_id', '$name_safe') ON DUPLICATE KEY UPDATE wfo_id=wfo_id;");
                }
                
                return true;
            }else{
                return false;
            }
        }

    }

}