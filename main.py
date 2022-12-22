#DB Params
DB_HOST = 'localhost'
DB_NAME = 'proyecto_db'
DB_USER = 'postgres'
DB_PASS = '123456'

import psycopg2
import psycopg2.extensions 
import psycopg2.extras
import datetime
import random
import string

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST , port=5432)
scheme = "oneM"

def genText():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def gen_random_date():
    start_date = datetime.date(2010, 1, 1)
    end_date = datetime.date(2020, 12, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)    

def generate_comprobante(size):
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                numero = 1000 + i
                random_date = gen_random_date()
                random_cost = random.randint(100, 1000000)
                random_type = (random.randint(1,2) == 1)
                curr.execute("SET search_path = '$user', " +  scheme)
                curr.execute(""" INSERT INTO comprobante (numero, fecha, precio, tipo) VALUES (%s, %s, %s, %s); """,(numero,random_date,random_cost,random_type))
                conn.commit()
    #End session                
    curr.close()

def poblate_by_clients(size):
    client_cache = []
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                id = 10000000 + i
                # client_cache.append(id)
                curr.execute("INSERT INTO " +scheme+ ".Empresa (ruc, direccion, razonSocial) VALUES("+str(id)+ ", ' " + genText()+ " ',"+ str(id) +" )")
                conn.commit()

    #End session                
    curr.close()

def poblate_by_warehouse(size):
    warehouse_cache = []

    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                
                dir = str(i)
                # while dir in warehouse_cache:
                #     dir = genText()
                #warehouse_cache.append(dir)
                curr.execute("INSERT INTO " +  scheme + ".almacen (direccion , capacidad ) VALUES( ' "+str(dir)+ " ' , " + str(random.randint(1000, 10000)) +" )")
                conn.commit()

    #End session                
    curr.close()

def gen_stock(size):
    keys_producto = get_keys(keys="id", table="producto", prob = 50, rows = size)
    keys_almacen = get_keys(keys="direccion", table="almacen", prob = 50, rows = size)
    sizeProducto = len(keys_producto) - 1
    sizeAlmacen = len(keys_almacen) - 1

    stock = {}
    
    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                cantidad = random.randint(500, 10000)
                id_producto = str(keys_producto[random.randint(0, sizeProducto)][0])
                id_almacen = str(keys_almacen[random.randint(0, sizeAlmacen)][0])
                if id_almacen in stock:
                    while id_producto in stock[id_almacen]:
                        id_producto = str(keys_producto[random.randint(0, sizeProducto)][0])
                else:
                    stock[id_almacen] = []        
                stock[id_almacen].append(id_producto)    
                curr.execute("INSERT INTO " +  scheme +  ".stock (id , almacen , cantidad) VALUES( '"+ id_producto + "' , '" + id_almacen + "'  ,   " + str(cantidad) + " )")
                conn.commit()
        #End session                
    curr.close()

def gen_compra(size):
    keys_cliente = get_keys(keys = "ruc", table = "empresa", rows = size, prob = 50)
    keys_producto =  get_keys(keys="id", table="producto", prob = 50, rows = size)
    keys_almacen = get_keys(keys="direccion", table="almacen", prob = 50, rows = size)
    keys = get_keys(keys = "numero", table ="comprobante", prob = 100)
    size_cliente = len(keys_cliente) - 1
    size_comprobante = len(keys) - 1
    size_producto = len(keys_producto) - 1
    size_almacen = len(keys_almacen) - 1

    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                ruc = str(keys_cliente[random.randint(0, size_cliente)][0])
                numero = str(keys[i][0])
                curr.execute("INSERT INTO "  + scheme + ".Compra (ruc, numero) VALUES ( "+ ruc + " , " + numero + " )" )
                cantidad = random.randint(1, 45)
                precioUnitario = random.randint(3, 750)
                curr.execute("INSERT INTO " + scheme +  ".CDetalle (id, direccion, numero, cantidad, precioUnitario) VALUES ( '" + str(keys_producto[random.randint(0,size_producto)][0]) + "' , '" + str(keys_almacen[random.randint(0,size_almacen)][0]) + "' , '" + numero + "' , '" + str(cantidad) + "' , '" + str(precioUnitario) + "' )")
                conn.commit()
    #End session                
    curr.close()
    gen_venta(size, keys)

def gen_venta(size, keys):
    keys_cliente = get_keys(keys = "ruc", table = "empresa", rows = size, prob = 50)
    keys_producto =  get_keys(keys="id", table="producto", prob = 50, rows = size)
    size_producto = len(keys_producto) - 1

    size_ruc = len(keys_cliente) - 1
    size_comprobante = len(keys) - 1

    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                numero =  str(keys[i + size][0])
                curr.execute("INSERT INTO " +  scheme + ".Venta (numero, ruc) VALUES ( '" + numero + "' , '" + str(keys_cliente[random.randint(0, size_ruc)][0]) + "' )")
                cantidad = random.randint(1, 45)
                precioUnitario = random.randint(3, 750)
                curr.execute("INSERT INTO " + scheme +  ".vDetalle (id, numero, cantidad, precioUnitario) VALUES ( '" + str(keys_producto[random.randint(0,size_producto)][0]) + "' , '" + numero + "' , '" + str(cantidad) + "' , '" + str(precioUnitario) + "' )")
                
                conn.commit()
    #End session                
    curr.close()           
    
def poblate_by_products(size):
    products_cache = []

    for i in range(size):
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                
                id = i
                curr.execute("INSERT INTO " + scheme + ".producto (id  , nombre ) VALUES( "+ str(id)+ "  ,  ' " + genText() +" ' )")
                conn.commit()
    #End session                
    curr.close()

def init():
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
            str = "SET search_path = '$user', " +  scheme + " ; delete from cdetalle; delete from vdetalle; delete from compra; delete from venta; delete from comprobante; delete from stock; delete from cliente; delete from persona; delete from producto; delete from almacen; delete from empresa;"
            curr.execute(str)
            conn.commit()

    curr.close()        


def get_keys(keys, table, rows = "", prob = 1):
    row = []
    if(str(rows) != ""):
        rows = "Limit " +  str(rows)

    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
            curr.execute("SET search_path = '$user', " +  scheme)
            curr.execute("SELECT " + str(keys)  +" FROM " + str(table)  + " TABLESAMPLE BERNOULLI( " + str(prob) + ") " + rows)
            row = curr.fetchall ()
            conn.commit()
    return row

if __name__ == "__main__":
    init()
    size = 1000000
    poblate_by_clients(size)
    print("Done client")
    poblate_by_products(size)
    print("Done product")
    poblate_by_warehouse(size)
    print("Done warehouse")
    gen_stock(size)
    print("Done stock")
    generate_comprobante(size * 2)
    print("Done comprobante")
    gen_compra(size)
    print("Done compra")
    conn.close()