For the purpose of testing recommendations systems, instead of working with a csv file lets us create a data set directly in a database. For our test we are working with local dB Postgresql version 9.3.5.

First lets create a schema
```sql
CREATE SCHEMA reco;
```

We could imagine having one table for user another one for movies(music or any other article) and a third table for critics

lets us create the tables, in a practical application we would make use of sharding if working with distributed DB (greenplum,...)
```sql
DROP TABLE IF EXISTS reco.users;
CREATE TABLE reco.users (
    user_id integer primary key,
    user_name text
);

DROP TABLE IF EXISTS reco.movies;
CREATE TABLE reco.movies (
    mv_id integer primary key,
    mv_name text
);

DROP TABLE IF EXISTS reco.critics;
CREATE TABLE reco.critics (
    user_id integer,
    mv_id integer,
    rating numeric
); 
```

let us fill some data 
```sql
INSERT INTO reco.users(user_id, user_name)
VALUES (1,'Lisa Rose'),
(2,'Gene Seymour'),
(3,'Michael Phillips'),
(4,'Claudia Puig'),
(5,'Mick LaSalle'),
(6,'Jack Matthews'),
(7,'Toby');

INSERT INTO reco.movies(mv_id,mv_name)
VALUES (1,'Lady in the Water'),
(2,'Snakes on a Plane'),
(3,'Just My Luck'),
(4,'Superman Returns'),
(5,'You, Me and Dupree'),
(6,'The Night Listener');

INSERT INTO reco.critics (user_id, mv_id, rating)
VALUES 
(1,1,2.5),
(1,2,3.5),
(1,3,3.0),
(1,4,3.5),
(1,5,2.5),
(1,6,3.0),

(2,1,3.0),
(2,2,3.5),
(2,3,1.5),
(2,4,5.0),
(2,5,3.5),
(2,6,3.0),

(3,1,2.5),
(3,2,3.5),
(3,4,3.5),
(3,6,4.0),

(4,2,3.5),
(4,3,3.0),
(4,4,4.0),
(4,5,2.5),
(4,6,4.5),

(5,1,3.0),
(5,2,4.0),
(5,3,2.0),
(5,4,3.0),
(5,5,2.0),
(5,6,3.0),

(6,1,3.0),
(6,2,4.0),
(6,4,5.0),
(6,5,3.5),
(6,6,3.0),

(7,2,4.5),
(7,4,4.0),
(7,5,1.0);
```

```sql
delete from reco.critics where user_id = 8;
delete from reco.users where user_id = 8;
```
