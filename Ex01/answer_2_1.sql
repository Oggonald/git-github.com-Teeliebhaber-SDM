CREATE TABLE Person (
	ID int NOT NULL PRIMARY KEY,
	name text,
	company text,
	age int);
CREATE TABLE Likes (
	ID1 int,
	ID2 int);
CREATE TABLE Friend (
	ID1 int,
	ID2 int);