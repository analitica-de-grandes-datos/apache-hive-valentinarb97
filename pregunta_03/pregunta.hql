/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
!hdfs dfs -rm -r -f /output;
DROP TABLE IF EXISTS data;
CREATE TABLE data (
    letra STRING,
    fecha DATE,
    valor INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH './data.tsv' OVERWRITE 
INTO TABLE data;
INSERT OVERWRITE DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT DISTINCT valor FROM data ORDER BY valor LIMIT 5;
!hdfs dfs -copyToLocal /output  output;


