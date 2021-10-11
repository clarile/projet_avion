import pandas as pd
from datetime import timedelta
from cartopy.crs import PlateCarree


def iterate_callsign(data):
    """Cree un iterable en fonction du callsign"""
    for _, dataframe in data.groupby(["callsign"]):
        yield dataframe


def iterate_icao24_callsign(data):
    """Cree un iterable en fonction du couple icao24-callsign(les départage)"""
    for _, dataframe in data.groupby(["callsign", "icao24"]):
        yield dataframe


def iterate_time(data, threshold):
    """Cree un iterable des vols par jour"""
    start = 0
    for i in (
        data.timestamp.diff()
        .dt.total_seconds()
        .loc[lambda x: x > threshold]
        .index
    ):
        yield data.loc[start : i - 1]
        start = i
    yield data.loc[start:]


def iterate_all(data, threshold):
    """Cree un iterable permettant d'accéder à chacun des vols"""
    for big_chunk in iterate_icao24_callsign(data):
        for little_chunk in iterate_time(big_chunk, threshold):
            yield little_chunk


class FlightCollection:
    def __init__(self, data):
        self.data = data

    def __repr__(self):
        return f"FlightCollection with {self.data.shape[0]} records of {self.data.shape[1]} values "

    @classmethod
    def read_json(cls, fichier):
        return cls(pd.read_json(fichier))

    def __iter__(self):
        for group in iterate_icao24_callsign(self.data):
            for elt in iterate_time(group, 20000):
                yield Flight(elt)

    def __len__(self):
        return sum(1 for _ in iterate_all(self.data, 20000))

    def __getitem__(self, colonne):
        before = 0
        after = 0
        if type(colonne) == str:
            if colonne in self.data["callsign"].values:
                # on nous donne un callsign
                new = FlightCollection(
                    self.data.query(f'callsign == "{colonne}"')
                )
                if len(new) > 1:
                    return new
                else:
                    return Flight(new.data)
            elif colonne in self.data["icao24"].values:
                # on nous donne un icao24
                new = FlightCollection(
                    self.data.query(f'icao24 == "{colonne}"')
                )
                if len(new) > 1:
                    return new
                else:
                    return Flight(new.data)
        elif type(colonne) == pd.Timestamp:
            before = colonne
            after = colonne + timedelta(days=1)
            # on nous donne un timestamp
            new = FlightCollection(
                self.data.query("@before < timestamp < @after")
            )
            if len(new) > 1:
                return new
            else:
                return Flight(new.data)
        # ce qu'on nous donne ne nous plait pas
        raise ValueError


class Flight:
    def __init__(self, data):
        self.data = data

    def min(self, colonne):
        return self.data[colonne].min()

    def max(self, colonne):
        return self.data[colonne].max()

    def __repr__(self):
        return f'Flight with the callsign : {self.data["callsign"].values[0]} and  aircraft icao24 : {self.data["icao24"].values[0]} on the {self.data["timestamp"].dt.date.values[0]}'

    def __lt__(self, autre_flight):
        return self.min("timestamp") < autre_flight.min("timestamp")

    def plot(self, ax):
        couleur = "lavenderblush"
        if self.decolle():
            couleur = "firebrick"
        if self.atterri():
            couleur = "darkgreen"
        # if self.passe():
        #    couleur = "lightblue"
        if not self.passe():
            self.data.query("latitude == latitude").plot(
                ax=ax,
                x="longitude",
                y="latitude",
                legend=False,
                transform=PlateCarree(),
                color=couleur,
                alpha=0.5,
            )

    @property
    def icao24(self):
        return self.data["icao24"].values[0]

    @property
    def callsign(self):
        return self.data["callsign"].values[0]

    def passe(self):
        if self.data["altitude"].mean() > 19000:
            return True
        return False

    def decolle(self):
        if self.data["vertical_rate"].mean() > 500:
            return True
        return False

    def atterri(self):
        if self.data["vertical_rate"].mean() < -500:
            return True
        return False
