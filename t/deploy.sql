CREATE TABLE EcologicalRisk
(
  id numeric(15, 0) NOT NULL,
  PRIMARY KEY( id ),
  classId numeric(5,0) NOT NULL,
  curies INT NULL
)

CREATE TABLE LegalPerson
(
  id numeric(15, 0) NOT NULL,
  PRIMARY KEY( id ),
  name VARCHAR(128) NULL,
  manager numeric(15, 0) NULL
)

CREATE TABLE NaturalPerson
(
  id numeric(15, 0) NOT NULL,
  PRIMARY KEY( id ),
  is_ref numeric(15, 0) NULL,
  firstName VARCHAR(128) NULL,
  name VARCHAR(128) NULL,
  ia_slot INT NULL,
  partner numeric(15, 0) NULL,
  age INT NULL,
  ia_ref numeric(15, 0) NULL
)

CREATE TABLE Person
(
  id numeric(15, 0) NOT NULL,
  PRIMARY KEY( id ),
  classId numeric(5,0) NOT NULL
)

CREATE TABLE a_children
(
  slot INT NULL,
  coll numeric(15, 0),
  item numeric(15, 0)
)

CREATE TABLE employees
(
  slot INT NULL,
  coll numeric(15, 0),
  item numeric(15, 0)
)

CREATE TABLE s_children
(
  coll numeric(15, 0),
  item numeric(15, 0)
)

CREATE TABLE OpalClass
(
        classId numeric(5,0) NOT NULL,
        className varchar(128),
        lastObjectId numeric(10, 0),
        PRIMARY KEY ( classId )
)

INSERT INTO OpalClass(classId, className, lastObjectId) VALUES (1, 'NuclearPlant', 0)

INSERT INTO OpalClass(classId, className, lastObjectId) VALUES (2, 'LegalPerson', 0)

INSERT INTO OpalClass(classId, className, lastObjectId) VALUES (3, 'NaturalPerson', 0)

