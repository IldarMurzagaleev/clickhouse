import glob
import clickhouse_connect


filenames = glob.glob("clustering_data/*.csv")
client = clickhouse_connect.get_client(host='localhost', username='default', password='', port=8123)
client.command('CREATE TABLE clust_data (value String) ENGINE Log')
data = []
for file in filenames:
    with open(file, encoding='utf-8', newline='') as tsvin:
        tsvin = tsvin.readlines()
        for row in tsvin:
            data.append([row])
print(len(data))
client.insert('clust_data', data, column_names=['value'])
result = client.query('SELECT * FROM clust_data')
print(len(result.result_set))

