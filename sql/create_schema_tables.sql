-- SCHEMA: bank

-- DROP SCHEMA IF EXISTS bank ;

CREATE SCHEMA IF NOT EXISTS bank
    AUTHORIZATION postgres;
	
-- Table: bank.Account

-- DROP TABLE IF EXISTS bank."Account";

CREATE TABLE IF NOT EXISTS bank."Account"
(
    "AccountId" bigint NOT NULL,
    "AccountNum" character(20) COLLATE pg_catalog."default",
    "ClientId" bigint,
    "DateOpen" date,
    CONSTRAINT account_pkey PRIMARY KEY ("AccountId")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS bank."Account"
    OWNER to postgres;
	
-- Table: bank.Client

-- DROP TABLE IF EXISTS bank."Client";

CREATE TABLE IF NOT EXISTS bank."Client"
(
    "ClientId" bigint NOT NULL,
    "ClientName" character varying(255) COLLATE pg_catalog."default",
    "Type" character(1) COLLATE pg_catalog."default",
    "Form" character varying(3) COLLATE pg_catalog."default",
    "RegisterDate" date,
    CONSTRAINT clients_pkey PRIMARY KEY ("ClientId")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS bank."Client"
    OWNER to postgres;	
	
	
-- Table: bank.List

-- DROP TABLE IF EXISTS bank."List";

CREATE TABLE IF NOT EXISTS bank."List"
(
    "ListName" character varying(50) COLLATE pg_catalog."default",
    "List" character varying(1000) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS bank."List"
    OWNER to postgres;

-- Table: bank.Operation

-- DROP TABLE IF EXISTS bank."Operation";

CREATE TABLE IF NOT EXISTS bank."Operation"
(
    "AccountDB" bigint,
    "AccountCR" bigint,
    "DateOp" date,
    "Amount" numeric(10,2),
    "Currency" character(3) COLLATE pg_catalog."default",
    "Comment" character varying(4000) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS bank."Operation"
    OWNER to postgres;
	
-- Table: bank.Rate

-- DROP TABLE IF EXISTS bank."Rate";

CREATE TABLE IF NOT EXISTS bank."Rate"
(
    "Currency" character(3) COLLATE pg_catalog."default",
    "Rate" numeric(10,2),
    "RateDate" date
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS bank."Rate"
    OWNER to postgres;
	
	
	
COPY  bank."Rate" FROM 'C:\tmp\first\Rate.csv' DELIMITER ';'  CSV HEADER;
COPY  bank."Operation" FROM 'C:\tmp\first\Rate.csv' DELIMITER ';'  CSV HEADER; 
COPY  bank."Client" FROM 'C:\tmp\first\Clients.csv' DELIMITER ';'  CSV HEADER;
COPY  bank."Account" FROM 'C:\tmp\first\Clients.csv' DELIMITER ';'  CSV HEADER;