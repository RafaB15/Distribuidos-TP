# Distribuidos-TP
Trabajo Practico N°1 de la materia de Sistemas Distribuidos. FIUBA 2024 2C

## Scope
El objetivo de este sistema es procesar 5 queries sobre un dataset de Juegos (Games) y Reseñas (Reviews) de Steam.

Las queries a resolver son las siguientes:

* Cantidad de juegos soportados en cada plataforma (Windows, Linux, MAC)
* Nombre de los juegos top 10 del género "Indie" publicados en la década del 2010 con más tiempo promedio historico de juego.
* Nombre de los juegos top 5 del género "Indie" con más reseñas positivas
* Nombre de juegos del género "Action" con mas de 5.000 reseñas negativas en idioma Inglés
* Nombre de juegos del género "Action" dentro del percentil 90 en cantidad de reseñas negativas

En esta instancia (y las subsiguientes) el objetivo es la resolucion de estas consultas junto con un sistema que soporte el incremento de los volumenes de cómputo para poder escalar el sistema.

## Vista Fisica

### Servicios implementados:
* Game Mapper:
  Recibe los Juegos que envia el cliente para procesar y mapea a solo los campos que son de utilidad para la resolucion de las consultas. Los campos son:
  * AppId
  * Name
  * Windows, MAC, Linux (consulta 1)
  * Género (ya sea Action o Indie para las consultas necesarias)
 
* Os Accumulator:

  
  El encargado de recibir los valores de los distintos sistemas operativos mapeados en el servicio antes mencionado (Game Mapper) el cual acumulará por sistema operativo la cantidad de juegos soportados en cada uno. Este tipo de nodo puede escalarse ya que cada nodo recibirá una parte de la información del Game Mapper y hará la acumulación parcial del total de los datos.
  Este servicio envía los datos una vez que el nodo anterior (Game Mapper) haya terminado de procesar los datos ya que requerimos de la información completa para dar los resultados al siguiente servicio (Os Final Accumulator). Esto se realiza con un EOF. Si el OS Accumulator recibe un EOF significa que está habilitado para enviar los datos acumulados.

* Os Final Accumulator:

  
  Es el encargado de unir todos los resultados parciales de los OS Accumulator. Este servicio conoce la cantidad de OS Accumulators que le estaran enviado informacion por lo que luego de recibir la N cantidad de EOFs que conincidirán con los N-OS Accumulators este servicio podrá enviar la información acumulada totalmente al servicio de Writer.

* Decade Filter:

  
  Es el servicio encargado de filtrar aquellos juegos que se hayan publicado en la década del 2010. Se asume, por el Game Mapper, que este servicio solo recibirá juegos de género Indie. Este servicio puede escalarse ya que los filtros pueden hacerse en instancias separadas y a medida que se van recibiendo los datos se van evaluando y luego se envían aquellos que cumplan con la condición de la década del 2010 al siguiente servicio. Como puede enviar resultados parciales este servicio para avisar que finalizó el filtro envía en EOF al siguiente servicio (Top 10 Avg Playtime).

* Top ten Accumulator:

  
  Es el encargado de filtar y acumular aquellos juegos que tengan más tiempo promedio de juego histórico. Este servicio no escala ya que siempre manejamos una cantidad de juegos reducida (unicamente 10) a medida que recibimos los juegos del servicio anterior entonces guardamos aquellos que unicamente esten entre los 10 mejores. Como este servicio está constantemente a la espera de la información del servicio anterior (Decade Filter) entonces necesita un EOF por cada parte escalada del cual está recibiendo información para poder enviar los datos definitivos. Este servicio enviará los 10 juegos con más tiempo promedio de juego al Writer para la devolución de resultados.

* English Filter:

  
  Recibe directo del entrypoint los datos que envía el cliente (unicamente Reseñas). Este servicio es el encargado de filtar aquellas reseñas que estén en Inglés.
  La detección del idioma se hizo con la biblioteca permitida por la cátedra: [Lingua](https://github.com/pemistahl/lingua-go). Este servicio puede escalarse ya que a medida que se evalua el idioma entonces acumulamos reseñas y enviamos información, cada procesamiento es independiente y no requerimos de toda la informacíon completa. Cabe destacar que este servicio no solo evalúa el idioma sino que también descarta los campos que ya no serán de utilidad para los siguientes procesamientos, como el texto de la reseña en sí. Una vez finalizado su procesamiento cada instancia del servicio enviará un mensaje de EOF al siguiente servicio.

* English Review Accumulator:

  
  Recibirá todas las reseñas de idioma inglés que fueron filtradas en el servicio anterior. Este servicio se encarga de acumular todas las reseñas por el Id y calculará de cada uno las reseñas positivas y negativas. Este servicio escala pero necesitamos que los Ids se envien a la misma instancia de este servicio (afinidad) para poder acumular por id. El envío por afinidad se hace mediante el cálculo de la Clave de Particionamiento. El servicio anterior (English Filter) tendrá en cuenta la cantidad de instancias de este servicio para calcular la clave de particionamiento para enviar los datos a la instancia correspondiente.

* Positive Review Filter:


  Recibirá las reseñas en inglés acumuladas. Este servicio se encarga de filtar aquellas reseñas que tengan mas de N reseñas positivas (en este caso 5.000). Este servicio escala ya que el procesamiento se puede hacer en distintas instancias del mismo servicio. Mientras se filtran las reseñas entonces estas se acumulan y se envían en paquetes para no causar un "cuello de botella" de enviar de a 1 reseña en la consulta.


* Joiners:

  Hay 3 servicios de Joiners en el Trabajo hecho. Lo que realizan es el Join (o match) entre los juegos y las reseñas. Estos esperan mensajes de juegos y luego de reseñas. Cada componente (ya sea juego o reseña) cuando se recibe se guarda en un Map con clave por ID. Cuando se recibe un juego o reseña que correponde a alguno ya almacenado se realiza el chequeo y si es correcto se hace el match y se envia al siguiente servicio.

  Este servicio escala para el procesamiento de datos. Para hacer coincidir y que se envíen los correspondientes datos a la instancia que corresponde. Esto se hace mediante la clave de particionamiento explicada en un servicio anterior que hace el envio con particionamiento por afinidad para que los Ids coincidan en una misma instancia de este servicio.

* Review Mapper:

  Es el análogo al servicio de Game Mapper. Filtra aquellos campos que son de importancia para las siguiente instancias de procesamiento tal como el Id y las reseñas. Este servicio puede escalar y procesar información en las distintas instancias que se levanten. 


* All review accumulator:

  Similar al servicio del acumulador de reviews. Este servicio también puede escalarse y procesar la información en diferentes instancias del servicio.


* Top Positive Reviews:

  Toma los juegos y las reseñas ya matcheadas y guardará aquellas que estén en el Top 5 de las reseñas positivas. Este servicio al igual que el Top 10 Avg Playtime no escala ya que siempre se guarda una cantidad minima de datos (5). A medida que se reciben más datos entonces se evalúa nuevamente si está entre los 5 con más reseñas y se guarda, caso contrario, se descarta. Este servicio requiere de los EOFs del Join anterior ya que para el envío y la finalización de procesamiento de los 5 juegos se requiere el conocimiento de la finalización completa del servicio anterior. Este servicio enviará su información al Writer.

  
* Percentile Accumulator:

  Este servicio es el encargado de calcular el percentil 90 de aquellas reseñas con más reseñas negativas. Este servicio no escala ya que requerimos de todas la información completa en el mismo lugar para realizar el calculo. Ordena los datos y calcula el percentil correspondiente. Al obtenerlo envía por clave de particionamiento al siguiente servicio que será el Join de la consulta que corresponda. 
  
## Diagrama de Robustez

[Link al Diagrama](https://github.com/RafaB15/Distribuidos-TP/blob/sofiajaves-patch-1/images/Diagrama%20de%20Robustez.png)

En este diagrama se pueden ver los distintos servicios mencionados en la seccion anterior pero con la conexión entre ellos. 

## Diagrama de Paquetes

[Link al Diagrama](https://github.com/RafaB15/Distribuidos-TP/blob/sofiajaves-patch-1/images/Paquetes.png)

En este diagrama se refleja la organización del código a gran escala por como estan organizadas por sus funcionalidades similares.

## DAG

[Link al Diagrama](https://github.com/RafaB15/Distribuidos-TP/blob/sofiajaves-patch-1/images/DAG.png)

En el DAG se pueden observar aquellos datos que son necesarios para procesar cada una de las consultas. Las consultas se pueden interpretar como los distintos caminos desde el entrypoint hasta el Post o Writer.

Es importante destacar que en esta instancia el cliente debe primero enviar todos los datos de juegos y luego al finalizar enviará las reseñas.


Desarrollado por Rafael Berenguel, Franco Gazzola y Sofia Javes
