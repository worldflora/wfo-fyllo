-- MySQL dump 10.13  Distrib 8.0.38, for macos14 (arm64)
--
-- Host: localhost    Database: wfo_facets
-- ------------------------------------------------------
-- Server version	8.4.2

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `facet_values`
--

DROP TABLE IF EXISTS `facet_values`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `facet_values` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'Possible value to have in a facet. Equivalent to a character state.',
  `facet_id` int NOT NULL,
  `name` varchar(100) NOT NULL,
  `description` text,
  `link_uri` varchar(255) DEFAULT NULL,
  `code` varchar(45) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `facet_id_idx` (`facet_id`),
  CONSTRAINT `facet_id` FOREIGN KEY (`facet_id`) REFERENCES `facets` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1929 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `facets`
--

DROP TABLE IF EXISTS `facets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `facets` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text,
  `link_uri` varchar(255) DEFAULT NULL,
  `heritable` tinyint DEFAULT '0',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `name_cache`
--

DROP TABLE IF EXISTS `name_cache`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `name_cache` (
  `wfo_id` varchar(15) NOT NULL,
  `name` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`wfo_id`),
  KEY `name` (`name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `snippets`
--

DROP TABLE IF EXISTS `snippets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `snippets` (
  `id` int NOT NULL AUTO_INCREMENT,
  `source_id` int NOT NULL,
  `wfo_id` varchar(14) NOT NULL,
  `body` text NOT NULL,
  `meta_json` json NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `wfo` (`wfo_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3356 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sources`
--

DROP TABLE IF EXISTS `sources`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sources` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'A source is a list binding WFO name IDs to facet values. It defines a subset of scores and also stores the external location of a file that can be harvested.',
  `name` varchar(100) NOT NULL,
  `description` text,
  `link_uri` varchar(255) DEFAULT NULL COMMENT 'A link to a human readable source for the the data.',
  `do_not_index` tinyint NOT NULL DEFAULT '0',
  `file_path` varchar(1000) DEFAULT NULL COMMENT 'The path within the file store (initially a GitHub repository) where the source file is located.',
  `oid` varchar(100) DEFAULT NULL COMMENT 'The GitHub oid (File version) last imported.',
  `facet_value_id` int DEFAULT NULL,
  `snippet_category` enum('link-out','image-jpeg','general','diagnostic','morphology','habit','cytology','physiology','size','lifespan','lifetime','biology','ecology','habitat','distribution','reproduction','conservation','use','dispersal','lifecycle','growth','genetics','chemistry','associations','population','management','legislation','threats','typematerial','typelocality','phylogeny','hybrids','literature','culture','vernacular') DEFAULT NULL,
  `snippet_language` enum('aar','abk','afr','aka','alb','amh','ara','arg','arm','asm','ava','ave','aym','aze','bak','bam','baq','bel','ben','bis','bos','bre','bul','bur','cat','cha','che','chi','chu','chv','cor','cos','cre','cze','dan','div','dut','dzo','eng','epo','est','ewe','fao','fij','fin','fre','fry','ful','geo','ger','gla','gle','glg','glv','gre','grn','guj','hat','hau','heb','her','hin','hmo','hrv','hun','ibo','ice','ido','iii','iku','ile','ina','ind','ipk','ita','jav','jpn','kal','kan','kas','kau','kaz','khm','kik','kin','kir','kom','kon','kor','kua','kur','lao','lat','lav','lim','lin','lit','ltz','lub','lug','mac','mah','mal','mao','mar','may','mlg','mlt','mon','nau','nav','nbl','nde','ndo','nep','nno','nob','nor','nya','oci','oji','ori','orm','oss','pan','per','pli','pol','por','pus','que','roh','rum','run','rus','sag','san','sin','slo','slv','sme','smo','sna','snd','som','sot','spa','srd','srp','ssw','sun','swa','swe','tah','tam','tat','tel','tgk','tgl','tha','tib','tir','ton','tsn','tso','tuk','tur','twi','uig','ukr','urd','uzb','ven','vie','vol','wel','wln','wol','xho','yid','yor','zha','zul','zzz') DEFAULT NULL,
  `last_import` timestamp NULL DEFAULT NULL COMMENT 'The last successful import of the file.',
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `modified` (`modified`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1805 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `username` varchar(45) DEFAULT NULL,
  `password_hash` varchar(255) DEFAULT NULL,
  `modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_unique` (`username`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `wfo_scores`
--

DROP TABLE IF EXISTS `wfo_scores`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `wfo_scores` (
  `wfo_id` varchar(15) NOT NULL,
  `value_id` int NOT NULL,
  `source_id` int NOT NULL,
  `meta_json` json DEFAULT NULL,
  `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`wfo_id`,`value_id`,`source_id`),
  UNIQUE KEY `one_source_value` (`wfo_id`,`value_id`,`source_id`) USING BTREE,
  KEY `wfo` (`wfo_id`),
  KEY `source_id` (`source_id`) USING BTREE,
  KEY `modified` (`modified`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-02-27 11:12:06
