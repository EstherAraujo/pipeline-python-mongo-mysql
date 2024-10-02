# pipeline-python-mongo-mysql

Data Pipeline para Extração, Transformação e Carregamento (ETL)
Este repositório contém um pipeline de dados que extrai informações de uma API, armazena os dados em um banco de dados MongoDB Atlas, transforma os dados e os salva como arquivos CSV, e finalmente insere dados relevantes em tabelas relacionais em um banco de dados MySQL.

> [!TIP]
> O link de conexão com o MongoDB Atlas, além dos nome e usuário do MySQL devem ser salvos em um arquivo .env para que sejam acessados. O arquivo .env deve estar na mesma pasta que os scripts e contem a seguinte estrutura:
>MONGODB_URI = "string com linha de conexão do MongoDB Atlas"
>DB_HOST = " string com o host usado pelo MySQL"
>DB_USERNAME = "string com o nome do usuário do MySQL"
>DB_PASSWORD = "string com a senha do usuário do MySQL"

O código está pronto para uso! Aproveitem!
