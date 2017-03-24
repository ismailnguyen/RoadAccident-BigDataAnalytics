## Proc�dure

R�cup�rer donn�es accidentologie paris (data.gouv.fr) en format CSV

Importer les donn�es CSV sur HDFS

Convertir les donn�es CSV en objet avec Pig

Filtrer pour ne garder seulement la localisation, la date, l'heure, le type des deux v�hicules

Grouper les donn�es par localisation

Grouper chaque localisation par type de v�hicule

Pour chaque ensemble localisation/v�hicule, y aff�cter la date/heure et le taux de pourcentage d'accident

Croiser les donn�es avec la m�t�o en fonction du lieu et de la date/heure
Croiser les donn�es avec la luminosit� en fonction du lieu et de la date/heure


##Cas d'usage :

- Identifier un jeu de donn�es (eg. Open Data)

- Stocker dans Hadoop

- Faire ressortir des conclusions sur ces donn�es


##Exemple

Application t�l�phone qui indique pourcentage accident en fonction de la localisation et du type de v�hicule
si possible pourcentage vers les direction nord sud est ouest

https://data.iledefrance.fr/explore/dataset/accidentologie-paris/table/?location=13,48.86239,2.34747&dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6ImFjY2lkZW50b2xvZ2llLXBhcmlzIiwib3B0aW9ucyI6e319LCJjaGFydHMiOlt7InR5cGUiOiJsaW5lIiwiZnVuYyI6IkFWRyIsInlBeGlzIjoiZGVwdCIsInNjaWVudGlmaWNEaXNwbGF5Ijp0cnVlLCJjb2xvciI6IiNlNzRjM2MifV0sInhBeGlzIjoiZGF0ZSIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6InllYXIiLCJzb3J0IjoiIn1dfQ%3D%3D