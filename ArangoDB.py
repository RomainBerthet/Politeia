from pyArango.connection import Connection
import pyArango
import logging
import yaml
import pandas as pd
from rich.console import Console
from rich.progress import Progress

class ArangoDB:

    def __init__(self, console=Console()):
        logging.getLogger(pyArango.__name__).disabled = True
        self.console = console
        try:
            f = 'anonymous.yml'
            file = open(f, 'r')
            params = yaml.safe_load(file)
            user = params['arangodb']['user']
            pwd = params['arangodb']['pwd']
            host = params['arangodb']['host']
            port = params['arangodb']['port']
            self.conn = Connection(arangoURL=f"http://{host}:{port}", username=user, password=pwd)
            self.db = self.conn[params['arangodb']['db']]
            self.is_valid = True
            self.console.log("La connexion à la base de donnée [bold cyan]ArangoDB[/bold cyan] est établie !")
        except Exception as e :
            self.is_valid = False
            self.console.log("[red] Une erreur est survenue lors de la connexion à la base de donnée ArangoDB[/red]")

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def executeAQL_Raw(self, aql, limit):
        try:
            return self.db.AQLQuery(aql, rawResults=True, batchSize=limit)
        except Exception as e:
            pass

    def execute(self, aql):
        try:
            return self.db.AQLQuery(aql, rawResults=True)
        except Exception as e:
            pass

    def import_csv(self, filename, index_col_name, collection_name):
        df = pd.read_csv(filename, sep=";", dtype=str, index_col=index_col_name)
        df['_key'] = df.index

        aql_dict = df.to_dict('records')

        with Progress() as progress:
            task = progress.add_task(
                f"Importation des documents dans la collection [bold cyan]{collection_name}[/bold cyan]",
                total=len(aql_dict))
            for bdv in aql_dict:
                clean_dict = {k: bdv[k] for k in bdv if not pd.isna(bdv[k])}
                self.execute(f"INSERT {clean_dict} IN {collection_name}")
                progress.advance(task)

    def save(self, data:dict, collection_name:str):
        key_dict = {"_key": data["_key"]}
        clean_dict = {k: data[k] for k in data if not pd.isna(data[k])}
        #print(f"UPSERT {key_dict} INSERT {clean_dict} UPDATE {clean_dict} IN {collection_name}")
        result = self.execute(f"UPSERT {key_dict} INSERT {clean_dict} UPDATE {clean_dict} IN {collection_name}")
        if result != []:
            self.console.log(f"[green]La requete d'insertion de [bold]{clean_dict['name']}[/bold] dans [bold]{collection_name}[/bold] s'est déroulée sans encombre.[/green]")
            return True
        else:
            self.console.log(f"[red]La requete d'insertion de [bold]{clean_dict['name']}[/bold] dans [bold]{collection_name}[/bold] a échouée.[/red]")
            return False

        