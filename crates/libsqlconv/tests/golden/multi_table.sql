-- MySQL dump fixture: multi-table test
-- Tests: multiple tables, various types, strings with special chars

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL DEFAULT '',
  `email` varchar(255) DEFAULT NULL,
  `bio` text,
  `age` tinyint(3) unsigned DEFAULT NULL,
  `score` decimal(10,2) DEFAULT '0.00',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_email` (`email`),
  KEY `idx_name` (`name`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'Alice','alice@example.com','Hello, I\'m Alice!\nNice to meet you.',25,99.50,1,'2024-01-15 10:30:00',NULL),(2,'Bob','bob@example.com','It''s a wonderful day; isn''t it?',30,150.75,1,'2024-02-20 14:00:00','2024-03-01 09:00:00'),(3,'Charlie',NULL,NULL,NULL,0.00,0,'2024-03-10 08:00:00',NULL);
INSERT INTO `users` VALUES (4,'Diana (test)','diana@example.com','She said: "hello"',28,200.00,1,'2024-04-01 12:00:00',NULL),(5,'Eve\\Bob','eve@example.com','back\\slash',22,50.00,1,'2024-05-01 00:00:00',NULL);
UNLOCK TABLES;

DROP TABLE IF EXISTS `posts`;
CREATE TABLE `posts` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `title` varchar(500) NOT NULL,
  `body` mediumtext,
  `status` enum('draft','published','archived') NOT NULL DEFAULT 'draft',
  `view_count` int(11) unsigned NOT NULL DEFAULT 0,
  `metadata` json DEFAULT NULL,
  `published_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user` (`user_id`),
  KEY `idx_status` (`status`),
  FULLTEXT KEY `ft_title_body` (`title`,`body`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `posts` WRITE;
INSERT INTO `posts` VALUES (1,1,'Hello World','This is my first post!','published',100,'{"tags": ["intro", "hello"]}','2024-01-16 10:00:00'),(2,1,'SQL Tips','Use semicolons; they''re important!','published',250,NULL,'2024-01-20 15:00:00'),(3,2,'(Untitled)','A post with (parentheses), "quotes", and \'apostrophes\'','draft',0,NULL,NULL);
UNLOCK TABLES;

DROP TABLE IF EXISTS `settings`;
CREATE TABLE `settings` (
  `key` varchar(100) NOT NULL,
  `value` longtext,
  `is_public` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `settings` WRITE;
INSERT INTO `settings` VALUES ('site_name','My \'Cool\' Site; v2.0',1),('motd','Line 1\nLine 2\nLine 3',1),('secret','p@$$w0rd!',0);
UNLOCK TABLES;

SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
