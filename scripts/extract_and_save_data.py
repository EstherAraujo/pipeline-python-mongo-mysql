from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env no ambiente de trabalho
load_dotenv()

# A função os.getenv é usada para obter o valor das variáveis de ambiente mongoBD primeira mente e depois no mysql
uri = os.getenv("MONGODB_URI")

def connect_mongo(uri): 
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return 

def create_connect_collection(db,col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    return requests.get(url).json()
    
def insert_data(col, data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":
    #estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo(uri)

    #cirando um bando de dados chamado "db_produtos" e uma coleção chamada "produtos"
    db = create_connect_db(client,"db_produtos")
    col = create_connect_collection(db,"produtos")
    
    #acessando a API para povoamento da coleção "produtos"
    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraídos: {len(data)}")

    #importando os dados da API para a coleção "produtos"
    n_docs = insert_data(col,data)
    print(f"\nQuantidade de documentos inseridos: {n_docs}")

    client.close()