from dataclasses import dataclass
from ArangoDB import ArangoDB
from models.GeoSpace import GeoSpace
from rich.console import Console

@dataclass
class Edge():
    entity_from: GeoSpace
    entity_to: GeoSpace
    edge_name:str
    db: ArangoDB
    console: Console = None
    data: dict = None
    rewrite: bool = False

    def __post_init__(self):
        if not self.console:
            self.console = Console()
        self.key = f"{self.entity_from.key}_{self.entity_to.key}"
        self.__save_edge_in_database()

    def __save_edge_in_database(self):
        """
        Cette fonction permet d'enregistrer un lien entre deux entités dans la base de données.
        :return: True si l'opération s'est correctement déroulée, False dans le cas inverse.
        """
        if not self.find_in_database() or self.rewrite:
            data_meta = {
                '_key': self.key
            }
            data_insertion = {
                '_key': self.key,
                '_from': f"{self.entity_from.collection_name}/{self.entity_from.key}",
                '_to': f"{self.entity_to.collection_name}/{self.entity_to.key}",
                'rel': f"{self.entity_from.collection_name.upper()}-->{self.entity_to.collection_name.upper()}"
            }
            if self.data:
                data_insertion = {**data_insertion, **self.data}
            aql_insert = f'UPSERT {data_meta} INSERT {data_insertion} UPDATE {data_insertion} IN {self.edge_name}'
            result = self.db.execute(aql_insert)
            if result:
                self.console.log(
                    f"[green]La requete d'insertion du lien entre [bold]{self.entity_from.name}[/bold] et [bold]{self.entity_to.name}[/bold] s'est déroulée sans encombre.[/green]")
                return True
            else:
                self.console.log(
                    f"[red]La requete d'insertion du lien entre [bold]{self.entity_from.name}[/bold] et [bold]{self.entity_to.name}[/bold] a échouée.[/red]")
                self.logger.error(
                    f"La requete d'insertion du lien entre {self.entity_from.name} et {self.entity_to.name} a échouée.")
                return False
        else:
            self.console.log(
                f"Le lien entre [cyan]{self.entity_from.name}[/cyan] et [cyan]{self.entity_to.name}[/cyan] est déjà enregistré dans la base de données.")
            return True

    def find_in_database(self) -> dict:
        try:
            return self.db.execute(f"RETURN DOCUMENT('{self.edge_name}/{self.entity_from.key}_{self.entity_to.key}')")[0]
        except:
            return {}