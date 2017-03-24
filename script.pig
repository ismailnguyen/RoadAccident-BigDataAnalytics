--hadoop fs -mkdir project

-- Vider tout avant de commencer
rm project;

-- Copier les données locales vers HDFS
copyFromLocal project/data/ project/data;

-- Charger les données d'accidents
accidents_brutes = LOAD 'project/data/accident' USING PigStorage(';') AS (date:chararray, heure:chararray, dept:chararray, ville:chararray, CP:chararray, adresse:chararray, adresse_complete:chararray, carr:chararray, lieu_1_nomv:chararray, lieu_2_nomv:chararray, vehicule_1_cadmin:chararray, vehicule_1_lveh:chararray, vehicle_2_cadmin:chararray, vehicule_2_lveh:chararray, vehicule_3_cadmin:chararray, vehicule_3_lveh:chararray, usager_1_catu:chararray, usager_1_grav:chararray, usager_1_lveh:chararray, usager_2_catu:chararray, usager_2_grav:chararray, usager_2_lveh:chararray, usager_3_catu:chararray, usager_3_grav:chararray, usager_3_lveh:chararray, usager_4_catu:chararray, usager_4_grav:chararray, usager_4_lveh:chararray, wgs84:chararray);

-- Ne garder que les champs utiles des données d'entrées
accidents_filtree = FOREACH accidents_brutes GENERATE  wgs84 AS position, date, heure, vehicule_1_cadmin AS vehicule_principal, vehicle_2_cadmin AS vehicule_secondaire;

-- Arrondir les acidents avec seulement 2 chiffres après la virgule
accidents_filtree = FOREACH accidents_filtree GENERATE FLATTEN(STRSPLIT(position, ', ')) AS (latitude:chararray, longitude:chararray), date, heure, vehicule_principal, vehicule_secondaire;

accidents_filtree = FOREACH accidents_filtree GENERATE SUBSTRING(latitude, 0, LAST_INDEX_OF(latitude, '.') + 3) AS latitude, SUBSTRING(longitude, 0, LAST_INDEX_OF(longitude, '.') + 3) AS longitude, date, heure, vehicule_principal, vehicule_secondaire;

-- Charger les données des radars
radars_brutes = LOAD 'project/data/radars' USING PigStorage(' ') AS (latitude:chararray, longitude:chararray, vitesse_limit:chararray, pays:chararray, code_pays:chararray, type_radar:chararray, nom_radar:chararray);

-- Filtrer radars (nettoyage)
radars_filtree = FOREACH radars_brutes GENERATE REPLACE(latitude, ',', '') AS latitude, REPLACE(longitude, ',', '') AS longitude, REPLACE(vitesse_limit, '@', '') AS vitesse_limit;

-- Arrondir les positions avec seulement 2 chiffres après la virgule
radars_filtree = FOREACH radars_filtree GENERATE SUBSTRING(latitude, 0, LAST_INDEX_OF(latitude, '.') + 3) AS latitude, SUBSTRING(longitude, 0, LAST_INDEX_OF(longitude, '.') + 3) AS longitude, vitesse_limit;

-- Joindres les données des radars (by positions)
accidents_par_radars = JOIN accidents_filtree BY (latitude, longitude), radars_filtree BY (latitude, longitude);

-- Générer un fichier Json avec les résultats
STORE accidents_par_radars INTO 'project/accidents_par_radars';
