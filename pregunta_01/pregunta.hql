/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

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
SELECT letra, count(*) AS valora FROM data GROUP BY letra;
!hdfs dfs -copyToLocal /output  output;