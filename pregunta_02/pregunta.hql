/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Construya una consulta que ordene la tabla por letra y valor (3ra columna).

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
SELECT letra,fecha,valor FROM data ORDER BY letra ASC,valor ASC, fecha ASC;
!hdfs dfs -copyToLocal /output  output;
