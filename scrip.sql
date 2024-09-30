-- MySQL dump 10.13  Distrib 8.0.38, for macos14 (arm64)
--
-- Host: 127.0.0.1    Database: cds
-- ------------------------------------------------------
-- Server version	5.7.44

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
-- Table structure for table `CustomsDeclaration`
--

DROP TABLE IF EXISTS `CustomsDeclaration`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CustomsDeclaration` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `CDS` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `CustomsOffice` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `C1` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `C2` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `C3` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `TPSMode` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `TradeType` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `DATE` date NOT NULL,
  `HOUR` time DEFAULT NULL,
  `DateUpdated` date DEFAULT NULL,
  `HourUpdated` time DEFAULT NULL,
  `ERC` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Exportor` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Importor` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ImportCountry` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `BLNumber` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Quantity` int(11) DEFAULT NULL,
  `UOM` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `GWeight` decimal(10,2) DEFAULT NULL,
  `WeightUOM` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `FinalDestination` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `POL` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Value` decimal(15,2) DEFAULT NULL,
  `TaxableValue` decimal(15,2) DEFAULT NULL,
  `TaxValue` decimal(15,2) DEFAULT NULL,
  `CDSLine` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Note` mediumtext COLLATE utf8mb4_unicode_ci,
  `CDSCompletedDate` date DEFAULT NULL,
  `CDSCompletedHour` time DEFAULT NULL,
  `CDSCancelDate` date DEFAULT NULL,
  `CDSCancelHour` time DEFAULT NULL,
  `Officer` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Officer2` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `HSCode` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Commodity` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `UnitQuantity` bigint(20) DEFAULT NULL,
  `UnitCost` double DEFAULT NULL,
  `InvoiceBL` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Currency` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `InvoiceValue` decimal(15,2) DEFAULT NULL,
  `TaxableValue2` decimal(15,2) DEFAULT NULL,
  `TaxUnit` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `TaxRate` decimal(5,2) DEFAULT NULL,
  `TaxClass` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Tax` decimal(15,2) DEFAULT NULL,
  `RefDoc1` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `RefDoc2` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `CreatedDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`Id`,`DATE`),
  KEY `idx_cds` (`CDS`),
  KEY `idx_tpsmode` (`TPSMode`),
  KEY `idx_tradetype` (`TradeType`),
  KEY `idx_date` (`DATE`),
  KEY `idx_importcountry` (`ImportCountry`),
  KEY `idx_hscode` (`HSCode`),
  KEY `idx_invoicebl` (`InvoiceBL`),
  KEY `idx_taxrate` (`TaxRate`)
) ENGINE=InnoDB AUTO_INCREMENT=2808839 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
/*!50100 PARTITION BY RANGE (TO_DAYS(DATE))
(PARTITION p2023_q1 VALUES LESS THAN (738976) ENGINE = InnoDB,
 PARTITION p2023_q2 VALUES LESS THAN (739067) ENGINE = InnoDB,
 PARTITION p2023_q3 VALUES LESS THAN (739159) ENGINE = InnoDB,
 PARTITION p2023_q4 VALUES LESS THAN (739251) ENGINE = InnoDB,
 PARTITION p2024_q1 VALUES LESS THAN (739342) ENGINE = InnoDB,
 PARTITION p2024_q2 VALUES LESS THAN (739433) ENGINE = InnoDB,
 PARTITION p2024_q3 VALUES LESS THAN (739525) ENGINE = InnoDB,
 PARTITION p2024_q4 VALUES LESS THAN (739617) ENGINE = InnoDB,
 PARTITION p2025_q1 VALUES LESS THAN (739707) ENGINE = InnoDB,
 PARTITION p2025_q2 VALUES LESS THAN (739798) ENGINE = InnoDB,
 PARTITION p2025_q3 VALUES LESS THAN (739890) ENGINE = InnoDB,
 PARTITION p2025_q4 VALUES LESS THAN (739982) ENGINE = InnoDB,
 PARTITION p2026_q1 VALUES LESS THAN (740072) ENGINE = InnoDB,
 PARTITION p2026_q2 VALUES LESS THAN (740163) ENGINE = InnoDB,
 PARTITION p2026_q3 VALUES LESS THAN (740255) ENGINE = InnoDB,
 PARTITION p2026_q4 VALUES LESS THAN (740347) ENGINE = InnoDB,
 PARTITION p2027_q1 VALUES LESS THAN (740437) ENGINE = InnoDB,
 PARTITION p2027_q2 VALUES LESS THAN (740528) ENGINE = InnoDB,
 PARTITION p2027_q3 VALUES LESS THAN (740620) ENGINE = InnoDB,
 PARTITION p2027_q4 VALUES LESS THAN (740712) ENGINE = InnoDB,
 PARTITION p2028_q1 VALUES LESS THAN (740803) ENGINE = InnoDB,
 PARTITION p2028_q2 VALUES LESS THAN (740894) ENGINE = InnoDB,
 PARTITION p2028_q3 VALUES LESS THAN (740986) ENGINE = InnoDB,
 PARTITION p2028_q4 VALUES LESS THAN (741078) ENGINE = InnoDB,
 PARTITION p2029_q1 VALUES LESS THAN (741168) ENGINE = InnoDB,
 PARTITION p2029_q2 VALUES LESS THAN (741259) ENGINE = InnoDB,
 PARTITION p2029_q3 VALUES LESS THAN (741351) ENGINE = InnoDB,
 PARTITION p2029_q4 VALUES LESS THAN (741443) ENGINE = InnoDB,
 PARTITION p2030_q1 VALUES LESS THAN (741533) ENGINE = InnoDB,
 PARTITION p2030_q2 VALUES LESS THAN (741624) ENGINE = InnoDB,
 PARTITION p2030_q3 VALUES LESS THAN (741716) ENGINE = InnoDB,
 PARTITION p2030_q4 VALUES LESS THAN (741808) ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-09-29 12:03:22

-- table ERC
CREATE TABLE ERC (
    Id BIGINT auto_increment PRIMARY KEY,
    ERC VARCHAR(255),
    ERC_Convert VARCHAR(255),
    ERC_Sub VARCHAR(255),
    Province_City VARCHAR(255),
    District_City VARCHAR(255),
    Ward VARCHAR(255),
    Province_City_Cao VARCHAR(255),
    District_City_Cao VARCHAR(255),
    Ward_Cao VARCHAR(255),
    Add VARCHAR(500),
    Phone VARCHAR(255)
) CHARACTER SET utf8mb4;
CREATE INDEX idx_erc ON ERC (ERC);
-- table Eximcode
CREATE TABLE Eximcode (
    TRADE VARCHAR(255),
    TradeType VARCHAR(10),
    TradeName VARCHAR(255),
    INSTRUCTION TEXT,
    KHAIKETHOP TEXT,
    GHICHU TEXT
) CHARACTER SET utf8mb4;
CREATE INDEX idx_eximcode ON Eximcode (TRADE);
-- table CustomsOffice
CREATE TABLE CustomerOffice (
    CustomsOffice VARCHAR(10),
    CustomsOfficeName VARCHAR(255),
    CustomsOfficeNameShort VARCHAR(50),
    CustomsOfficeName2 VARCHAR(255),
    MaDoi VARCHAR(10),
    Province_City VARCHAR(100),
    District_City VARCHAR(100),
    CucHaiQuan VARCHAR(100),
    CustomsOfficeMaster VARCHAR(10),
    Note TEXT
) CHARACTER SET utf8mb4;
CREATE INDEX idx_customeroffice ON CustomerOffice (CustomsOffice);
