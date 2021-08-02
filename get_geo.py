from ArangoDB import ArangoDB
from models.GeoSpace import Pays, Region
from models.Edge import Edge
from rich.console import Console
import logging
import OSMPythonTools
logging.getLogger(OSMPythonTools.__name__).disabled = True

if __name__ == '__main__':
    console = Console()
    db = ArangoDB(console=console)

    france = Pays(name="France", key="FR", db=db, console=console)
    bfc = Region(key="FR-BFC", name="Bourgogne Franche-Comté", db=db, console=console)
    fr_bfc_link = Edge(entity_from=france, entity_to=bfc, db=db, edge_name="GeoLink", console=console)
    # for region in france.get_regions():
    #     region.find_in_database()
