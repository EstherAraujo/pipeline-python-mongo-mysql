from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import os
from dotenv import load_dotenv
import pandas as pd

# Carrega as variáveis do arquivo .env no ambiente de trabalho
load_dotenv()

# A função os.getenv é usada para obter o valor das variáveis de ambiente mongoBD primeira mente e depois no mysql
uri = os.getenv("MONGODB_URI")

def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({},{"$rename":{f"{col_name}":f"{new_name}"}})

def  select_category(col, category):
    query = {"Categoria do Produto": f"{category}"}
    lista_categoria = []
    
    for doc in col.find(query):
        lista_categoria.append(doc)
    return lista_categoria

def make_regex(col, regex):
    query = {"Data da Compra":{"$regex":f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    return lista_regex

def create_dataframe(lista):
    df = pd.DataFrame(lista)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format = "%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

def save_csv(df, path):
    df.to_csv(path,index=False)
    print(f"\nO arquivo {path} foi salvo.")

if __name__ == "__main__":
    #estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo(uri)
    db = create_connect_db(client,"db_produtos")
    col = create_connect_collection(db,"produtos")
    
    #renomeando as colunas latitude e longitude
    rename_column(col,"lon","Longitude")
    rename_column(col,"lat","Latitude")

       
    #Selecionar e salvar um csv apenas com os documentos cuja categoria do produto seja "livros" com o formato de data corrigido para ser aceito pelo SQL
    lista_livros = select_category(col, "livros" )
    df_livros = create_dataframe(lista_livros)
    format_date(df_livros)
    save_csv(df_livros, "../data/tabela_livros.csv")

    #Selecionar e salvar um csv apenas com os documentos com datas de venda maior a partir de 2021 com o formato de data corrigido para ser aceito pelo SQL 
    lista_produtos = make_regex(col,"/202[1-9]")
    df_produtos = create_dataframe(lista_produtos)
    format_date(df_produtos)
    save_csv (df_produtos,"../data/tabela2021_em_diante.csv")

    client.close()