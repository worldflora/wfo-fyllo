<?php

class IndexerSwitch{

    /**
     * Returns null if the switch is off
     * or 24 hours in the same
     * 
     */
    public static function onUntil(){
        
        global $mysqli;

        $response = $mysqli->query("SELECT * FROM `modification_log` ORDER BY `modified` DESC LIMIT 1;");
        $row = $response->fetch_assoc();

        if($row['wfo_id'] == 'INDEXER_OFF'){
            // we are flagged as off so return null
            return null;
        }else{
            // we may be on if the last change was within 24 hours
            $dt = new DateTime($row['modified']);
            $dt->add(DateInterval::createFromDateString('1 day'));

            $now = new DateTime();

            if($now < $dt){
                // we are before the deadline
                $difference = $now->diff($dt);
                return $difference->format("%H hrs %I minutes");
            }else{
                // we have passed the deadline
                return null;
            }

        }
  
    }

    public static function toggle(){

        global $mysqli;

        if(IndexerSwitch::onUntil()){
            $new_position = "INDEXER_OFF";
        }else{
            $new_position = "INDEXER_ON";
        }

        $mysqli->query("INSERT INTO modification_log (`wfo_id`, `modified`) VALUES ('{$new_position}', now()) ON DUPLICATE KEY UPDATE `modified` = now();");

    }


}