# ACC Hotlap merger
# SPDX-License-Identifier: MIT

import argparse
import json
import csv
import zipfile
from io import TextIOWrapper
from typing import Any, Dict, List, Tuple

from cars import car_models

def ms_to_time_format(milliseconds: int) -> str:
    """Convert milliseconds to mm:ss.ms format."""
    minutes = milliseconds // 60000
    seconds = (milliseconds % 60000) // 1000
    milliseconds = milliseconds % 1000
    return f"{minutes:02}:{seconds:02}.{milliseconds:03}"

def process_session_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process session data and return best laps grouped by playerId and carModel."""
    laps: List[Dict[str, Any]] = data["laps"]
    leaderboard: List[Dict[str, Any]] = data["sessionResult"]["leaderBoardLines"]

    # Create a mapping of (carId, driverIndex) to (playerId, carModel)
    driver_map: Dict[Tuple[int, int], Tuple[str, int]] = {
        (entry["car"]["carId"], idx): (
            driver["playerId"],
            entry["car"]["carModel"],
        )
        for entry in leaderboard
        for idx, driver in enumerate(entry["car"]["drivers"])
    }

    # Map playerID to driver names
    player_names: Dict[str, str] = {
         driver["playerId"]: (driver["firstName"], driver["lastName"])
         for entry in leaderboard
         for driver in entry["car"]["drivers"]
    }

    # Extract the best lap for each (playerId, carModel) pair
    best_laps: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for lap in laps:
        key = driver_map.get((lap["carId"], lap["driverIndex"]))
        if not key:
            continue
        if lap["isValidForBest"] and (
            key not in best_laps or lap["laptime"] < best_laps[key]["laptime"]
        ):
            best_laps[key] = {
                "laptime": lap["laptime"],
                "splits": lap["splits"],
                "name": player_names[key[0]]
            }

    return best_laps


def aggregate_results(zip_file: str) -> List[Dict[str, str]]:
    """Aggregate best laps across multiple files."""
    aggregated_results: Dict[Tuple[str, int], Dict[str, Any]] = {}

    with zipfile.ZipFile(zip_file, "r") as z:
        # Iterate through all JSON files in the ZIP
        for filename in z.namelist():
            if filename.endswith(".json"):
                with z.open(filename) as file:
                  session_data = json.load(file)

            session_best_laps = process_session_data(session_data)

            # Update the aggregated results
            for key, lap_info in session_best_laps.items():
                if (key not in aggregated_results
                    or lap_info["laptime"] < aggregated_results[key]["laptime"]):
                    aggregated_results[key] = lap_info

    best_lap = min(aggregated_results.items(),
                   key=lambda x: x[1]["laptime"])[1]["laptime"]

    # Convert aggregated results to a list and sort by lap time
    return sorted(
        [
            {
                "player_id": player_id,
                "name": data["name"],
                "car_model": car_model,
                "laptime": data["laptime"],
                "splits": data["splits"],
                "gap": data["laptime"] - best_lap
            }
            for (player_id, car_model), data in aggregated_results.items()
        ],
        key=lambda x: x["laptime"],
    )


def export_hotlaps(output_csv: str, data: List[Dict[str, str]]) -> None:
    """Write aggregated data to CSV"""
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Name", "Car", "Lap Time", "S1", "S2", "S3", "Gap"]
        out_data: List[Dict[str, Any]] = [
            {
                "Name": entry["name"][0] + " " + entry["name"][1],
                "Car": car_models[entry["car_model"]],
                "Lap Time": ms_to_time_format(entry["laptime"]),
                "S1": ms_to_time_format(entry["splits"][0]),
                "S2": ms_to_time_format(entry["splits"][1]),
                "S3": ms_to_time_format(entry["splits"][2]),
                "Gap": f'{entry["gap"] / 1000:.3f}'
            }
            for entry in data
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_data)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Aggregate best laps from JSON files in a ZIP archive.")
    parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Path to the input ZIP file containing JSON session data."
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Path to the output CSV file."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    aggregated_data = aggregate_results(args.input)
    export_hotlaps(args.output, aggregated_data)
