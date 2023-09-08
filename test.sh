out=$(psql "host=test_postgres sslmode=disable user=myuser dbname=mydb port=5434" -c 'show clients where server_id = spqr_shard_1:6432 and dbname = db2;')
test "$out" = " client_id | user | dbname | server_id | router_address | router_time*** | shard_time*** 
-----------+------+--------+-----------+----------------+-----------------+----------------
(0 rows)" || {
    echo "where with AND should work"
    exit 1
}