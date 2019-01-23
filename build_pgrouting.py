import psycopg2


conn = psycopg2.connect(host="localhost", database="DATABASE_NAME", user="postgres", password="")
cur = conn.cursor()
print("connected to database")

cur.execute("SELECT MIN(gid), MAX(gid) FROM streets;")
min_id, max_id = cur.fetchone()
print(f"there are {max_id - min_id + 1} edges to be processed")
cur.close()

interval = 200000
for x in range(min_id, max_id+1, interval):
    cur = conn.cursor()
    cur.execute(
    f"select pgr_createTopology('streets', 0.000001, 'geom', 'gid', rows_where:='gid>={x} and gid<{x+interval}');"
)
    conn.commit()
    x_max = x + interval - 1
    if x_max > max_id:
        x_max = max_id
    print(f"edges {x} - {x_max} have been processed")

cur = conn.cursor()
cur.execute("""ALTER TABLE streets_vertices_pgr
  ADD COLUMN IF NOT EXISTS lat float8,
  ADD COLUMN IF NOT EXISTS lon float8;""")

cur.execute("""UPDATE streets_vertices_pgr
  SET lat = ST_Y(the_geom),
      lon = ST_X(the_geom);""")

conn.commit()
