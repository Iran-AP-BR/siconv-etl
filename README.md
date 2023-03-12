# Siconv-ETL
Extração, transformação e carga de dados coletados do repositório do Siconv e reaorganizados em um novo modelo de dados simplificado (python3.8)

## Uso
---
Para utilizar, basta adionar a pasta (módulo) `etl` ao projeto e assegurar a instalação das dependências listadas em `requirements.txt`.
O módulo `etl` pode ser carregado a seguinte forma:

~~~python
from etl import ETL
~~~

Onde `ETL` é a classe responsável por executar o pipeline de extração, transformação e carga dos dados. Essa classe possui um método chamado `pipeline()`, o qual se encarrega de executar as três etapas em sequência.

Na etapa de extração os dados são baixados do repositório na internet para arquivos intermediários no formato `.parquet`, de modo a assegurar que os dados já baixados sejam aproveitados em caso de retomada da extração após uma tentativa mal-sucedida. Esses arquivos ficam armazenados no caminho indicado pela propriedade `STAGE_FOLDER` da classe config.

Na etapa de transformação, os dados armazenados em `STAGE_FOLDER` são manipulados de modo a se acomodarem ao modelo predefinido. O resultado é armazenado no caminho indicado pela propiedade `DATA_FOLDER` da classe config.

A etapa de carregamento somente é executada se uma instância de classe derivada da classe abstrata `LoaderClass` for passada como argumento de inicialização da instância da classe `ETL`.


### Loaders
---
O módulo `etl` não possui uma implementação de caragadores (loaders). Por isso, uma classe deve ser implementada a partir de uma classe abstrata denominada `LoaderClass` e passada para a `ETL`, caso contrário apenas a extração e a transformação serão realizadas.

A classe abstrata `LoaderClass` é definida conforme a seguir:

~~~python
from abc import ABC, abstractmethod
from .data_files_tools import FileTools

class LoaderClass(ABC):
    @abstractmethod
    def __init__(self, **kwargs) -> None:
        self.file_tools = FileTools()
    
    @abstractmethod
    def load(self) -> None:
        pass

    def read_data(self, table_name) -> any:
        return self.file_tools.read_data(table_name=table_name)

~~~

Essa abstração indica que os métodos `__init__()` e `load()` devem ser obrigatoriamente sobreescritos. O método `read_data()` é utilizado para ler os arquivos resultantes das transformações dos dados e, em princípio, não precisa ser sobreescrito.

Vale destacar que o carregamento consiste apenas em ler os arquivos (tabelas) com os dados transformados e convertê-los para o formato desejado, incluisve com transformações adicionais, se necessário, e envio ao destino, que pode ser arquivos ou bancos de dados locais ou em serviços na nuvem. A título de simples exemplo, pode-se criar uma classe `JSONLoader`, conforme código a seguir, cuja finalidade é somente carregar os dados no formato `json` em arquivos em uma pasta local:

~~~python
from etl import rows_print, feedback, LoaderClass
from pathlib import Path


class JSONLoader(LoaderClass):
    def __init__(self, path, logger) -> None:
        super().__init__()
        self.logger = logger
        self.path = path

    def load(self):
        self.logger.info('[Loading to JSON]')
        
        Path(self.path).mkdir(parents=True, exist_ok=True)

        self.__load_table__("estados")
        self.__load_table__("municipios")
        self.__load_table__("proponentes")
        self.__load_table__("emendas")
        self.__load_table__("emendas_convenios")
        self.__load_table__("licitacoes")
        self.__load_table__("calendario")
        self.__load_table__("convenios")
        self.__load_table__("fornecedores")
        self.__load_table__("movimento")
        self.__load_table__("atributos")
        self.__load_table__("data_atual")

        self.logger.info('[JSON load complete]')

    def __load_table__(self, table_name):
        feedback(self.logger, label=f'-> {table_name}', value='updating...')

        try:
        
            table = self.read_data(table_name=table_name)
            table.to_json(Path(self.path).joinpath(f'{table_name}.json'), 
                          orient='records', force_ascii=False)
        
        except Exception as e:

            raise Exception(f'JSONLoader: {str(e)}')  
                      
        feedback(self.logger, label=f'-> {table_name}', value=f'{rows_print(table)}')

~~~

Nessa implementação, o método `__load_table__()` foi criado, por conveniência, como uma forma genérica de ler e carregar as tabelas, já que, nessa caso, o procedimento é o mesmo para todas as tabelas. No entanto, podem ser criados tantos métodos de carregamento quantos forem necessários, inclusive um para cada tabela.


## Variáveis de ambiente:
---
Essas variáveis de ambiente contém informações essenciais ao processo e são recuperadas por meio da classe `Config`. Podem ser manipuladas por meio do sistma operacional ou utilizando arquivos `.env` em conjunto com a biblioteca `python-dotenv`. Não há necessidade de alterar os valores padrões.

- `CURRENT_DATE_URI`, indica o endereço na internet do arquivo que contém a data carga dos dados no repositório. O valor padrão é http://repositorio.dados.gov.br/seges/detru/data_carga_siconv.csv.zip.

- `CURRENT_DATE_URI_COMPRESSION`, indica o tipo de compressão utilizada no arquivo da data carga dos dados no repositório, seu padrão é 'zip'

- `DOWNLOAD_URI`, indica o endereço raiz na internet onde os arquivos de dados estão disponíveis, seu valor padrão é  http://repositorio.dados.gov.br/seges/detru/.

- `MUNICIPIOS_BACKUP_FOLDER`, indica o local onde estão os arquivos com os dados dos municípios e estados. O padrão é a sub pasta `municipios` localizada na mesma pasta onde está `config.py`.

- `NLTK_DATA`, indica onde a pasta `stemmers` está localizada. O padrão é a mesma pasta onde está `config.py`. Nessa pasta estão os arquivos auuxiliares no tratamento de textos com aprendizagem de máquina. Servem para retirar das palavas afixos e deixar apenas seus radicais, de modo a permitir a classificação de textos. 

- `MODEL_PATH`, indica o local onde está armazenado o arquivo com o modelo de aprendizagem de maquina para classificação de risco dos convênios. O padrão é `trained_model/model.pickle`, na mesma pasta onde está `config.py`.

- `DATA_FOLDER`, indica o local onde os dados transformados são armazenados. O padrão é `data_files/end_files`, localizada um nível acima da pasta onde está `config.py`.

- `STAGE_FOLDER`, indica o local onde os dados extraídos são armazenados. O padrão é `data_files/stage_files`, localizada um nível acima da pasta onde está `config.py`.

- `TIMEZONE_OFFSET`, indica a diferença, em horas, entre o Brasil e o local de hospedagem da aplicação. O padrão é `-3`. Esse valor é importante para assegurar a verificação de atualidade dos dados.


Além das variáveis de ambiente a classe `Config`, disponibiliza as seguintes constantes: 


- `COMPRESSION_METHOD` igual a `gzip`, método de compressão utilizado nos arquivos `.parquet`.

- `FILE_EXTENTION`, igual a `.parquet`, indica a extensão do nome dos arquivos de dados.

- `CURRENT_DATE_FILENAME` igual a `data_atual.parquet`. Nome do arquivo que contém a data atual dos dados.
    
- `CHUNK_SIZE` igual a `500000`. Número de linhas utilizadas nos casos de leitura fragmentada de dados, para otimização do uso da memória. 


## Teste
---
O script `test.py` representa uma demonstração da utilização. Esse script faz uso das classes `JSONLoader` `MySQLLoader`, criadas apenas como exemplo, e que realizam carregamentos dos dados para arquivos `.json` e para um banco de dados `MySQL`. Importante salientar que para realizar o carregamento dos dados para o formato ou tecnologia desejada, deve-se criar uma classe específica, baseada na classe abstrata `LoaderClass`.

~~~python
from loaders.json_loader import JSONLoader
from loaders.mysql_loader import MySQLLoader
from etl import ETL, getLogger
from pathlib import Path
from etl.config import Config
from etl.data_files_tools import FileTools
from sqlalchemy_utils import database_exists
import sqlalchemy as sa
from os import getenv
import pandas as pd


SQLALCHEMY_DATABASE_URI = getenv('SQLALCHEMY_DATABASE_URI', 
                                 default='mysql+pymysql://root:123456@siconvdata:3306/siconvdata')
CHUNK_SIZE = getenv('CHUNK_SIZE', default=500000)

class DB(object):
    def __init__(self) -> None:
        self.engine = None
        self.db_uri = getenv('SQLALCHEMY_DATABASE_URI', 
                                 default='mysql+pymysql://root:123456@siconvdata:3306/siconvdata')

    def connect_database(self):
        self.engine = None
        if database_exists(self.db_uri):
            self.engine = sa.create_engine(self.db_uri)
    
    def check_database(self):
        current_date_table = 'data_atual'
        if sa.inspect(self.engine).has_table(current_date_table):
            return self.engine.execute(f'select DATA_ATUAL from {current_date_table}').scalar()

        raise Exception(f'Tabela {current_date_table} não localizada.')

    def write(self, data_frame, table_name):
        rows_count = 0
        nrows = len(data_frame)
        self.engine.execute(f'truncate table {table_name};')
        for index in range(0, nrows, CHUNK_SIZE):
            df = data_frame[index:index + CHUNK_SIZE]
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            rows_count += len(df)
        return rows_count
    

if __name__ == '__main__':
    path = str(Path(__file__).parent.joinpath('json_loads'))
    
    json_loader = JSONLoader(path=path, logger=getLogger())
    mysql_loader = MySQLLoader(database=DB(), logger=getLogger())
    etl = ETL(loaders=[json_loader, mysql_loader], config=Config(), file_tools=FileTools(), pd=pd)
    etl.pipeline(force_download=False, force_transformations=False)

~~~

Nesse teste, as classes `ETL`, `JSONLoader` e `MySQLLoader` são importadas. Em seguida, as classe `JSONLoader` e`MySQLLoader` são instanciadas nos objetos `json_loader` e `mysql_loader`, respectivamente. No caso de `json_loader`, pode ser passado o caminho onde os arquivos `.json` devem ser armazenados, enquanto que no caso do `mysql_loader` é passado uma unstância da classe `DB`. Além disso, am ambos os casos é passado um logger (pode ser um logger qualquer, inclusive um disponível no módulo `etl`).

A classe `DB` foi criada para executar as funções de manipulação do banco de dados, usando a técnica de injeção de dependência por meio do argumento `database`. Contudo, essas funções também pode ser incorporadas na própria classe `MySQLLoader`. O mesmo vale para o `path`, no caso da classe `JSONLoader`.

Em resumo, as classes `loaders` podem ser construídas da forma que os desenvolvedor desejar, desde que seja derivada da classe abstrata `LoaderClass`.

<br/>

A classe `ETL` é instanciada no objeto `etl`, com `loader` como argumento. Em seguida, o método `etl.pipelione()` e executado. Esse método aceita 2 argumentos opcionais:

- `force_download`, cujo valor padrão é `False`, e tem a finalidade de definir se o download dos dados (extração) deve ser realizado, mesmo que os dados em `STAGE_FOLDER` estejam atualizados;
- `force_transformations`, cujo valor padrão é `False`, e determina se a etapa de transformação deve ser executada mesmo que já existam dados transformados atualizados.

Esse teste pode ser executado em qualquer ambiente (virtual ou não) com python 3.8, desde que todas as dependências seja devidamente instaladas. Por outro lado, a execução pode ser feita com auxílio do `docker`, cujas configurações já estão prontas para assegurar o funcionamento do código em ambiente isolado sem afetar o sistema operacional. 


## Usando o docker-compose
---
> _deve ser executado na pasta raiz do projeto, onde estão os arquivos `Dockerfile` e `docker-compose.yml`_

#### _Build_

```
sudo docker-compose build
```

#### _Run_
```
 docker-compose up
```
