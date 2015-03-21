/*
SQLyog Community Edition- MySQL GUI v5.27
Host - 5.1.41 : Database - trafficvisualization
*********************************************************************
Server version : 5.1.41
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

create database if not exists `trafficvisualization`;

USE `trafficvisualization`;

/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

/*Table structure for table `coordinates` */

DROP TABLE IF EXISTS `coordinates`;

CREATE TABLE `coordinates` (
  `driver_name` varchar(30) DEFAULT NULL,
  `driver_id` varchar(30) DEFAULT NULL,
  `route_name` varchar(30) DEFAULT NULL,
  `route_id` varchar(30) DEFAULT NULL,
  `truck_id` varchar(30) DEFAULT NULL,
  `timestamp` varchar(30) DEFAULT NULL,
  `longitude` varchar(30) DEFAULT NULL,
  `latitude` varchar(30) DEFAULT NULL,
  `violation` varchar(30) DEFAULT NULL,
  `total_violations` varchar(30) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

/*Data for the table `coordinates` */

insert  into `coordinates`(`driver_name`,`driver_id`,`route_name`,`route_id`,`truck_id`,`timestamp`,`longitude`,`latitude`,`violation`,`total_violations`) values ('driver4','4','7','7','4','2014-07-01 18:00:58','-73.49771','40.743247',NULL,'1'),('driver3','3','6','6','3','2014-07-01 18:00:58','-74.190585999999939','41.346879999999913','0','2'),('driver2','2','5','5','2','2014-07-01 18:00:58','-74.038678','41.501975','0','1'),('driver1','1','4','4','1','2014-07-01 18:15:35','-74.27449700000011','41.349813999999924','33','2');

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
