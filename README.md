# pipeline-python-mongo-mysql

__Data Pipeline para Extração, Transformação e Carregamento (ETL)__ </br>
Este repositório contém um pipeline de dados que extrai informações de uma API, armazena os dados em um banco de dados MongoDB Atlas, transforma os dados e os salva como arquivos CSV, e finalmente insere dados relevantes em tabelas relacionais em um banco de dados MySQL.</br>
A estrutura do projeto é a seguinte:</br> 
pipeline-python-mongo-mysql/</br> 
├── data/</br> 
│   ├── tabela_livros.csv</br> 
│   └── tabela2021_em_diante.csv</br> 
├── extract_and_save_data.py</br> 
├── transform_data.py</br> 
└── save_data_mysql.py</br> 
├── .env</br> 
└── README.md</br> 

> [!TIP]
> O link de conexão com o MongoDB Atlas, além dos nome e usuário do MySQL devem ser salvos em um arquivo .env para que sejam acessados. O arquivo .env deve estar na mesma pasta que os scripts e contem a seguinte estrutura:</br>
>MONGODB_URI = "string com linha de conexão do MongoDB Atlas"</br>
>DB_HOST = " string com o host usado pelo MySQL"</br>
>DB_USERNAME = "string com o nome do usuário do MySQL"</br>
>DB_PASSWORD = "string com a senha do usuário do MySQL"</br>

O código está pronto para uso! Aproveitem!
