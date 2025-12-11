Docker Compose file:

Erstellt eine redis datenbank für europe eine für asia
Erstellt zwei Edge nodes einmal für asia einmal für europe 

Beide subscriben auf farm/topic über den hivemq broker -> beide bekommen alle daten von allen iot geräten
Da edge europe nur für europe zuständig ist, ignoriert er alle daten von device_inda device , device_brazil und device_egypt, diese werden von edge asia verwaltet(obwohl brazil und egypt nicht in asien sind lol)

dies kann in edge_config.py angepasst werden 

alle messungen werden mit timestamp in redis abgelegt, es können zeitfenster-queries abgefragt werden(Cloud-responsibility?)

edge macht anomalie erkennung pro sensorwert, Schwellenwert kann in edge_config.py angepasst werden. 

Aggregationen über Zeitfenster:
bündelt mehrere messungen pro device in einem bestimmten Zeitfenster(kann ebenfalls in edge_config.py angepasst werden)
und speichert sie in redis 

Status- und Metrikverwaltung:
Edge schreibt ihren eigenen Status und Device‑/Node‑Metriken in Redis (online/offline, letzte Aktivität, Anzahl Readings etc.)
 damit man im Monitoring (Redis Commander) sehen kannst, wie die Edge‑Schicht gesundheitlich aussieht


