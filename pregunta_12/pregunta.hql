/*

Pregunta
===========================================================================

Escriba una consulta que compute la cantidad de registros por letra de la 
columna 2 y clave de la columna 3; esto es, por ejemplo, la cantidad de 
registros en tienen la letra `a` en la columna 2 y la clave `aaa` en la 
columna 3 es:

    a    aaa    5

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/
DROP TABLE IF EXISTS tbl0; 
DROP TABLE IF EXISTS datos; 
CREATE TABLE tbl0 ( 
    c1 STRING, 
    c2 ARRAY<CHAR(1)>,  
    c3 MAP<STRING, INT> 
    ) 
    ROW FORMAT DELIMITED  
        FIELDS TERMINATED BY '\t' 
        COLLECTION ITEMS TERMINATED BY ',' 
        MAP KEYS TERMINATED BY '#' 
        LINES TERMINATED BY '\n'; 
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE tbl0; 
 
CREATE TABLE datos AS SELECT letra, clave, valor FROM (SELECT letra, c3 FROM tbl0 LATERAL VIEW explode(c2) tbl0 AS letra ) data
LATERAL VIEW explode (c3) data; 
INSERT OVERWRITE LOCAL DIRECTORY './output' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT letra, clave, COUNT(1) FROM datos GROUP BY letra, clave ;