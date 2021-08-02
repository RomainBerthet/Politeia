from dataclasses import dataclass, field
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass, OverpassResult
from shapely.geometry import shape
import json
from typing import List, Generator
from ArangoDB import ArangoDB
from abc import ABC
from rich.console import Console

@dataclass
class GeoSpace(ABC):
    key: str
    name: str
    db: ArangoDB
    console: Console = None
    new_data:dict = None
    rewrite: bool = False
    result:OverpassResult = None

    def __post_init__(self):
        self.nominatim = Nominatim()
        self.overpass = Overpass()
        if not self.console:
            self.console = Console()
        self.__save_object()

    def __find_osm_id(self) -> Nominatim:
        return self.nominatim.query(f"{self.name}, France", zoom=self.zoom).areaId()

    def _fetch_data(self) -> OverpassResult:
        result = None
        try:
            with self.console.status(f"[green]Récuperation des données de [bold]{self.name}[/bold] via l'API d'OpenStreetMap...") as status:
                while result is None:
                    query = overpassQueryBuilder(
                            area=self.__find_osm_id(),
                            elementType=['relation'],
                            selector=['"type"="boundary"', *self.selector],
                            includeGeometry=True)
                    result = self.overpass.query(query)
                    return result.elements()[0]
        except:
            self.console.log(f"[red]Impossible de récuperer les données de [bold]{self.name}[/bold].[/red]")

    def __get_geometry(self, overpass_result:OverpassResult) -> dict:
        try:
            geometry = overpass_result.geometry()
            centroid = shape(json.loads(str(geometry))).centroid
            lat = centroid.x
            long = centroid.y
        except:
            lat = ""
            long = ""
            geometry = ""
            self.console.log(f"[red]Impossible de récuperer le contour géometrique de [bold]{self.name}[/bold].[/red]")
        return {'latitude': lat, 'longitude':long, 'geometry':geometry}

    def __get_object(self) -> dict:
        if not self.find_in_database() or self.rewrite:
            if not self.result:
                result = self._fetch_data()
            else:
                result = self.result
            data = result.tags()
            geometry = self.__get_geometry(result)
            return {"_key":self.key, **data, **geometry}
        else:
            self.console.log(f'{self.collection_name} [cyan]{self.name}[/cyan] est déja enregistré(e) dans la base de données')
            return self.find_in_database()

    def __save_object(self):
        data = self.__get_object()
        if self.new_data:
            data = {**data, **self.new_data}
        if self.rewrite or self.new_data:
            self.db.save(data=data, collection_name = self.collection_name)
        return self

    def add_new_data(self, new_data:dict):
        self.new_data = new_data
        return self.__save_object()

    def find_in_database(self) -> dict:
        try:
            return self.db.execute(f"RETURN DOCUMENT('{self.collection_name}/{self.key}')")[0]
        except:
            return {}


@dataclass
class Commune(GeoSpace):
    zoom: int = 10
    selector: List[str] = field(default_factory=lambda:['"admin_level"="8"','"boundary"="administrative"'])
    collection_name: str = "Commune"


@dataclass
class Departement(GeoSpace):
    zoom: int = 8
    selector: List[str] = field(default_factory=lambda:['"admin_level"="6"','"boundary"="administrative"'])
    collection_name: str = "Departement"


@dataclass
class Region(GeoSpace):
    zoom: int = 5
    selector: List[str] = field(default_factory=lambda:['"admin_level"="4"','"boundary"="administrative"'])
    collection_name: str = "Region"


@dataclass
class Pays(GeoSpace):
    collection_name: str = "Pays"

    def _fetch_data(self) -> OverpassResult:
        result = None
        try:
            with self.console.status(f"[green]Récuperation des données de [bold]{self.name}[/bold] via l'API d'OpenStreetMap...") as status:
                while result is None:
                    query = f'rel["name"~"{self.name}"][admin_level="2"][type="boundary"][boundary="administrative"]; out body geom;'
                    result = self.overpass.query(query)
                    return result.elements()[0]
        except:
            self.console.log(f"[red]Impossible de récuperer les données de [bold]{self.name}[/bold].[/red]")

    def get_regions(self) -> Generator:
        query = 'rel["ISO3166-2"~"^FR"][admin_level="4"][type="boundary"][boundary="administrative"]; out body geom;'
        regions = Overpass().query(query).elements()
        for r in regions:
            reg = Region(name=r.tag('name'), key=r.tag('ISO3166-2'), db=self.db, result=r)
            yield reg