# clickhouse
1 step: initial_load_db.py - загрузка tsv файла в базу данных в виде строк -> prods (clickhouse table)


2 step: db_to_datamart.py - выгрузка датасета из базы данных в tsv файл для его обработки витриной данных -> selected_data.csv


3 step: datamart.scala - обработка датасета витриной данных на scala и отсечение столбцов признаков, которые были отмечены как 
несущественные при анализе данных датасета -> preprocessed_data (folder)


4 step: model.py - загрузка данных от витрины, кластеризация и сохранение в tsv формате для отправки в базу данных -> clustering_data(folder)


5 step: save_to_db.py - данные от витрины, дополненные столбцом с меткой кластера, загружаются в новую таблицу и могут быть дальше использованы в работе по классификации -> clust_data (clickhouse table)
