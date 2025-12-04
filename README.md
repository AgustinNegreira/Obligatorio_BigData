# Obligatorio_BigData

En este repositorio se encuentra todo el codigo desarrollado para la implementacion de un DataLake junto con un pipeline de transformación de datos presentado como solución a la consigna obligatoria planteada en la asignatura Big Data en el marco de la Universidad Católica del Uruguay.

Este repositorio contiene los siguientes directorios:

### datalake

Allí se encuentra la estructura de carpetas que representa las zonas del datalake, las cuales en este caso contienen un archivo de texto de ejemplo para que se pueda subir a GitHub y no desaparezcan las carpetas vacías, pero la idea sería tener la información en sus distintos estados a lo largo de todas las zona del datalake.

### kpis

En este directorio se encuentra el output de la planilla excel con los datos de los KPIs definidos en el la última etapa del pipeline de procesado de datos. Siempre que ejecutemos este último paso, los resultados irán a este directorio.

### metadata

Allí se encuentra la información correspondiente al modelo de metadatos definido para la información con la que se trabaja a lo largo del datalake.

### scripts

En este directorio se encuentran los scripts que componen los pasos del pipeline de datos implementado en python, que toman los archivos de los directorios del directorio datalake para procesarlos según se vayan ejecutando estos scripts.