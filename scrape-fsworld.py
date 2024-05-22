#!/usr/bin/env python

import csv
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import time
from typing import Union
import requests
import bs4

DATASETS = {"FSE": "https://fs-world.org/E", "FSC": "https://fs-world.org/C"}
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
EVENT_DIR = OUTPUT_DIR / "events"
EVENT_DIR.mkdir(exist_ok=True)
print(f"Saving data to {OUTPUT_DIR}")

CarKind = Enum("CarKind", ["ELECTRIC", "COMBUSTION", "OTHER"])
EventKind = Enum("EventKind", ["ELECTRIC", "COMBUSTION", "MIXED"])


@dataclass
class PlacementData:
    place: int
    score: float


DisciplineData = Union[PlacementData, None]


@dataclass
class TeamData:
    name: str
    country: str
    kind: CarKind

    place: int
    cost: DisciplineData
    bp: DisciplineData
    ed: DisciplineData
    acc: DisciplineData
    sp: DisciplineData
    autox: DisciplineData
    endu: DisciplineData
    eff: DisciplineData
    pen: float
    total: float


@dataclass
class EventData:
    id: int
    kind: EventKind
    name: str
    results: list[TeamData]


class Scraper:
    def __init__(self, min_pause: float = 1):
        self.min_pause = min_pause
        self.last_request = 0.0

    def get(self, url: str) -> bs4.BeautifulSoup:
        now = time.time()
        diff = now - self.last_request
        if diff < self.min_pause:
            time.sleep(self.min_pause - diff)
        self.last_request = time.time()
        return bs4.BeautifulSoup(requests.get(url).content, "html.parser")


def extract_discipline_data(col: bs4.Tag) -> DisciplineData:
    text = col.text.rstrip(".")
    if text == "-":
        return None
    place = int(col.text.rstrip("."))
    tooltip = col.find(lambda tag: tag.has_attr("title"))
    assert isinstance(tooltip, bs4.Tag)
    try:
        title = tooltip["title"]
    except KeyError as e:
        print(col)
        raise e
    assert isinstance(title, str)
    score = float(title)
    return PlacementData(place, score)


events: dict[int, EventData] = {}

scraper = Scraper()

for wrl_kind, url in DATASETS.items():
    soup = scraper.get(url)
    event_select = soup.find(id="WorldEvents")
    assert isinstance(event_select, bs4.Tag)
    event_ids = map(lambda x: int(x["value"]), event_select.find_all("option"))
    for event_id in event_ids:
        if event_id in events:
            print(
                f"Skipping {wrl_kind}/{event_id} (already scraped, probably a mixed event)"
            )
            continue

        event_url = f"{url}/{event_id}"
        soup = scraper.get(event_url)

        h4 = soup.find_all("h4")
        assert len(h4) == 1
        h4 = h4[0]
        assert isinstance(h4, bs4.Tag)
        event_name = h4.text
        event_info = h4.next_sibling
        assert event_info is not None
        event_info = event_info.lower()
        if "mixed event" in event_info:
            event_kind = EventKind.MIXED
        elif "pure electric" in event_info:
            event_kind = EventKind.ELECTRIC
        elif "pure combustion" in event_info:
            event_kind = EventKind.COMBUSTION
        print(f"{wrl_kind}/{event_id}: {event_name} ({event_kind})")

        results_header = soup.find(id="results")
        assert isinstance(results_header, bs4.Tag)
        table = results_header.find_next_sibling("table", class_="wrl_table")
        assert isinstance(table, bs4.Tag)
        rows = table.find_all("tr")

        results: list[TeamData] = []
        for row in rows:
            assert isinstance(row, bs4.Tag)
            if row.find("th") is not None:
                continue

            cols = row.find_all("td")
            assert len(cols) == 12
            country, name = cols[0]["title"].split(" | ", 1)
            place = int(cols[1].text.rstrip("."))
            cost = extract_discipline_data(cols[2])
            bp = extract_discipline_data(cols[3])
            ed = extract_discipline_data(cols[4])
            acc = extract_discipline_data(cols[5])
            sp = extract_discipline_data(cols[6])
            autox = extract_discipline_data(cols[7])
            endu = extract_discipline_data(cols[8])
            eff = extract_discipline_data(cols[9])
            pen = float(cols[10].text)
            total = float(cols[11].text)
            kind = cols[11]["title"]
            if kind == "electric":
                kind = CarKind.ELECTRIC
            elif kind == "combustion":
                kind = CarKind.COMBUSTION
            elif kind == "other":
                kind = CarKind.OTHER
            else:
                raise ValueError(f"Unknown car kind: {kind}")

            results.append(
                TeamData(
                    name,
                    country,
                    kind,
                    place,
                    cost,
                    bp,
                    ed,
                    acc,
                    sp,
                    autox,
                    endu,
                    eff,
                    pen,
                    total,
                )
            )

        events[event_id] = EventData(event_id, event_kind, event_name, results)

        with open(EVENT_DIR / f"{event_id}.csv", "w") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "name",
                    "country",
                    "kind",
                    "place",
                    "cost_place",
                    "cost_score",
                    "bp_place",
                    "bp_score",
                    "ed_place",
                    "ed_score",
                    "acc_place",
                    "acc_score",
                    "sp_place",
                    "sp_score",
                    "autox_place",
                    "autox_score",
                    "endu_place",
                    "endu_score",
                    "eff_place",
                    "eff_score",
                    "pen",
                    "total",
                ],
            )
            writer.writeheader()
            for team in results:
                writer.writerow(
                    {
                        "name": team.name,
                        "country": team.country,
                        "kind": team.kind.name,
                        "place": team.place,
                        "cost_place": team.cost.place if team.cost else None,
                        "cost_score": team.cost.score if team.cost else None,
                        "bp_place": team.bp.place if team.bp else None,
                        "bp_score": team.bp.score if team.bp else None,
                        "ed_place": team.ed.place if team.ed else None,
                        "ed_score": team.ed.score if team.ed else None,
                        "acc_place": team.acc.place if team.acc else None,
                        "acc_score": team.acc.score if team.acc else None,
                        "sp_place": team.sp.place if team.sp else None,
                        "sp_score": team.sp.score if team.sp else None,
                        "autox_place": team.autox.place if team.autox else None,
                        "autox_score": team.autox.score if team.autox else None,
                        "endu_place": team.endu.place if team.endu else None,
                        "endu_score": team.endu.score if team.endu else None,
                        "eff_place": team.eff.place if team.eff else None,
                        "eff_score": team.eff.score if team.eff else None,
                        "pen": team.pen,
                        "total": team.total,
                    }
                )

    with open(OUTPUT_DIR / f"{wrl_kind}.csv", "w") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["id", "kind", "name", "teams"],
        )
        writer.writeheader()
        for event in events.values():
            writer.writerow(
                {
                    "id": event.id,
                    "kind": event.kind.name,
                    "name": event.name,
                    "teams": len(event.results),
                }
            )
