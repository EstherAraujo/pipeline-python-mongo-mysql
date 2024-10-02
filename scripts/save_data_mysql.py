
import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd

# Carrega as variáveis do arquivo .env no ambiente de trabalho
load_dotenv()

# A função os.getenv é usada para obter o valor das variáveis de ambiente mongoBD primeira mente e depois no mysql
host = os.getenv("DB_HOST")
user = os.getenv("DB_USERNAME")
senha = os.getenv("DB_PASSWORD")

def connect_mysql(host_name, user_name, pw): 
    cnx = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = pw
    )
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor,db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    print(f"\nBase de dados {db_name} criada.") 

def show_databases(cursor):
    cursor.execute("SHOW DATABASES;")
    for db in cursor:
        print (db)

def create_product_table(cursor,db_name,tb_name):
    cursor.execute(
    f""" 
    CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
        id VARCHAR(100),
        Produto VARCHAR(100),
        Categoria_Produto VARCHAR(100),
        Preco FLOAT(10,2),
        Frete FLOAT(10,2),
        Data_Compra DATE,
        Vendedor VARCHAR(100),
        Local_Compra VARCHAR(100),
        Avaliacao_Compra INT,
        Tipo_Pagamento VARCHAR(100),
        Qntd_Parcelas INT,
        Longitude FLOAT(10,2),
        Latitude FLOAT(10,2),

        PRIMARY KEY (id)
    );  
    """)
    print(f"\nTabela {tb_name} foi criada na base de dados {db_name}.") 
    
def show_tables(cursor,db_name):
    cursor.execute(f"USE {db_name};")
    cursor.execute("SHOW TABLES;")
    for tb in cursor:
        print(tb)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_product_data(cnx,cursor,df,db_name,tb_name):
    lista = [tuple(row) for i,row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cursor.executemany(sql,lista)
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}")
    cnx.commit()

if __name__ == "__main__":
    #conectando ao servidor MySQL
    cnx = connect_mysql(host, user, senha)

    #criando um cursor
    cursor = create_cursor(cnx)

    #criando um banco de dados
    create_database(cursor,"db_produtos")

    #exibindo todos os banco de dados existentes
    show_databases(cursor)

    #criar duas tabelas chamadas "tb_livros" e "tb_2001_em_diante" no banco de dados criado
    create_product_table(cursor,"db_produtos","tb_livros")
    create_product_table(cursor, "db_produtos","tb_2001_em_diante")

    #exibir todas as tabelas no banco de dados criados
    show_tables(cursor,"db_produtos")

    #ler os dados dos arquivos csv "tb_livros.csv" e "tabela2021_em_diante.csv"
    df1 = read_csv("../data/tabela_livros.csv")
    df2 = read_csv("../data/tabela2021_em_diante.csv")
                   
    #adicionar os dados lidos às tabelas criadas
    add_product_data(cnx,cursor,df1,"db_produtos","tb_livros")
    add_product_data(cnx,cursor,df2,"db_produtos","tb_2001_em_diante")
    #finalizar conexão com o MySQL

    cnx.close()    



