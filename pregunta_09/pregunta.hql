/*

Pregunta
===========================================================================

Escriba una consulta que retorne la columna `tbl0.c1` y el valor 
correspondiente de la columna `tbl1.c4` para la columna `tbl0.c2`.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data1.csv' INTO TABLE tbl1;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/

!hdfs dfs -rm -r -f /output;
DROP TABLE IF EXISTS datos_final;

CREATE TABLE datos_final
AS
    SELECT
    t0.c1,
    t0.c2,
    t1.c4_2
FROM
    tbl0 t0
JOIN (
       SELECT
            c1, c4_1, c4_2
            FROM
            tbl1 
            LATERAL VIEW explode(c4) tbl1 AS c4_1, c4_2
    ) t1
ON
    (t0.c1 = t1.c1 AND t0.c2 = t1.c4_1);

INSERT OVERWRITE DIRECTORY '/output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    *
FROM
    datos_final;

!hdfs dfs -copyToLocal /output  output;