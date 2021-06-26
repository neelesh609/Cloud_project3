import textwrap
from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
import pyodbc
import timeit
from time import time
import redis
import hashlib
import pickle
from pymongo import MongoClient




driver = '{ODBC Driver 17 for SQL Server}'

server_name = 'nxs0682'
database_name = 'neeldatabase'

server = '{server_name}.database.windows.net,1433'.format(server_name=server_name)

username = "nxs0682"
password = "Aakneel@216"

connection_string = textwrap.dedent('''
    Driver={driver};
    Server={server};
    Database={database};
    Uid={username};
    Pwd={password};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
'''.format(
    driver=driver,
    server=server,
    database=database_name,
    username=username,
    password=password
))

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
r = redis.StrictRedis(host='neel0609.redis.cache.windows.net',port=6380, db=0, password='zrK0jyTQRRMqMCv+uq8z3ZR94PIBSa1303pQYb4Gd6Q=', ssl=True)
result = r.ping()
client = MongoClient(r"mongodb://neel0609:jxP9m47PCx069cemuOXNKQ5g7wyY06c8WD1n5lq1p4Hnx5m169MXxkqCwj4fzCkaBLNwYPGJ4wuj0pVfntaRDA==@neel0609.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@neel0609@")
db = client.neelesh
todos = db.all_month

app=Flask(__name__)

@app.route('/', methods=['GET' ,'POST'])
def eq_count1():
    return render_template('index.html')


@app.route('/create_tb', methods=['GET','POST']) #change the query
def create_tb():
   cstart = time()
   sql1="CREATE TABLE eqt_3(Time nvarchar(50), Latitude float, Longitude float, Depth float, Mag float NULL DEFAULT 0.0, Magtype nvarchar(50) NULL DEFAULT 0.0, Nst int NULL DEFAULT 0.0, Gap float NULL DEFAULT 0.0, Dmin  float NULL DEFAULT 0.0, Rms float, Net nvarchar(50), ID nvarchar(50), Updated nvarchar(50), Place nvarchar(MAX), Type nvarchar(50), HorizontalError float NULL DEFAULT 0.0, DepthError float, MagError float NULL DEFAULT 0.0, MagNst int NULL DEFAULT 0.0, Status nvarchar(50), LocationSource nvarchar(50), MagSource nvarchar(50))"
   sql2="create index myindex ON eqt_3(id,latitude,longitude,mag);"
   cursor.execute(sql1)
   cursor.execute(sql2)
   conn.commit
   cend = time()
   executiontime = cend - cstart
   print(executiontime)
   return render_template('index.html', ctime= executiontime)

@app.route("/rand_query" , methods=['GET','POST'])
def rand_query():
  lower_mag = '1'
  upper_mag = '5'
  rand_cnt = str(request.args.get('frandomcount'))
  time_before = time()
  for x in range(int(rand_cnt)):
   cursor.execute("select time,mag,id, place from all_month where mag >= "+lower_mag+" and mag <= "+upper_mag+";")
   fetchres = cursor.fetchall()
  time_after = time()
  time_taken = time_after - time_before
  print("time_before "+str(time_before))
  print("time_before "+str(time_after))
  return render_template('index.html', settimetaken=time_taken)



@app.route("/lim_query" , methods=['GET','POST'])
def lim_query():
  rand_lim = str(request.args.get('frandomcount'))
  before_time = time()
  for x in range(int(rand_lim)):
   cursor.execute("select time,mag,id, place from all_month where mag = 5.8;")
   fetchres = cursor.fetchall()
  after_time = time()
  timetaken = after_time - before_time
  return render_template('index.html', set_timetaken=timetaken)

@app.route("/Redis_query1" , methods=['GET','POST'])
def Redis_query1():
  
 
  getRandom=str(request.args.get('fimprandomcount'))
  lower_mag = '1'
  upper_mag = '2'
  
  query= "select time,mag,id, place from all_month where mag >= "+lower_mag+" and mag <= "+upper_mag+";"
  hash = hashlib.sha224(query.encode('utf-8')).hexdigest()
  key = "redis_cache:" + hash
  starttime=time()
  for i in range(int(getRandom)):
    if (r.get(key)):
      print("redis cached")
    else:
      print(key)
      print(r)
      cursor.execute(query)
      fetchimpres = list(cursor.fetchall())
      r.set(key, pickle.dumps(list(fetchimpres)))
      r.expire(key, 36)

    cursor.execute(query)
  endtime = time()
  executiontime = endtime - starttime
  return render_template('index.html', setimprovedtime=executiontime)


@app.route("/radis_query2" , methods=['GET','POST'])
def radis_query2():
  print("Ping returned : " + str(result))
  getRandom=str(request.args.get('fimprandomcount'))
  
  query= "select time,mag,id, place from all_month where mag = 3;"
  hash = hashlib.sha224(query.encode('utf-8')).hexdigest()
  key = "redis_cache:" + hash
  starttime=time()
  for i in range(int(getRandom)):
    if (r.get(key)):
      print("redis cached")
    else:
      print(key)
      print(r)
      cursor.execute(query)
      fetchimpres = list(cursor.fetchall())
      r.set(key, pickle.dumps(list(fetchimpres)))
      r.expire(key, 36)

    cursor.execute(query)
  endtime = time()
  executiontime = endtime - starttime
  return render_template('index.html', set_improvedtime=executiontime)

@app.route('/mongo_query1', methods=['GET', 'POST'])
def mongo_query1():
    if request.method == 'POST':
        num = request.form.get('mongo_query1')
    startmag = 2
    stopmag = 4
    starttime=time()
    for data in range(int(num)):
        todo = todos.find({"Mag": startmag})
        print(todo)
        
    endtime = time()
    executiontime = endtime - starttime
    return render_template("index.html", set_executiontime = executiontime)

@app.route('/mongo_query2', methods=['GET', 'POST'])
def mongo_query2():
    if request.method == 'POST':
        num = request.form.get('mongo_query2')
    startmag = 1
    stopmag = 5
    starttime=time()
    for data in range(int(num)):
        todo = todos.find({"Mag": startmag})
        print(todo)
        
    endtime = time()
    executiontime = endtime - starttime
    return render_template("index.html", setexecutiontime = executiontime)
  



if __name__=="__main__":
    app.run(debug=True)
