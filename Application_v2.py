#!/usr/bin/env python
# coding: utf-8

# In[16]:


"""
NeTEx Fare Finder - Fares.zip (Drive -> in-memory) edition

Requirements:
- Place your service account key as 'credentials.json' in the same folder as this script.
- Upload a file named 'Fares.zip' to the root of your Google Drive (My Drive) and share it
  with the service account email (client_email in credentials.json).
- The ZIP should contain your XML fare files and Routes.xlsx / Stops.xlsx (or files named similarly).

This script downloads the single ZIP into memory, extracts files in-memory, and runs the app.
"""

import os
import io
import json
import base64
import gzip
import traceback
import zipfile
import xml.etree.ElementTree as ET
import tkinter as tk
from tkinter import ttk
import pandas as pd

# Google Drive client libs
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except Exception:
    raise RuntimeError("Missing google client libraries. Install with: "
                       "py -m pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")

# --------------------------
# CONFIG
# --------------------------
SERVICE_ACCOUNT_FILENAME = "netex-fare-finder-1dfe0a831d82.json"
ZIP_FILE_NAME_ON_DRIVE = "Fares.zip"
EMBEDDED_ROUTES_B64 = ""  # optional: base64(gzip) of Routes.xlsx
EMBEDDED_STOPS_B64 = ""   # optional: base64(gzip) of Stops.xlsx
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# --------------------------
# Service account resolution (robust)
# --------------------------
def find_service_account_file():
    candidate_paths = []
    env_path = os.environ.get("SERVICE_ACCOUNT_FILE")
    if env_path:
        candidate_paths.append(env_path)
    candidate_paths.append(os.path.join(os.getcwd(), SERVICE_ACCOUNT_FILENAME))
    try:
        script_dir = os.path.dirname(__file__)
        if script_dir:
            candidate_paths.append(os.path.join(script_dir, SERVICE_ACCOUNT_FILENAME))
    except NameError:
        pass
    candidate_paths.append(os.path.abspath(SERVICE_ACCOUNT_FILENAME))
    candidate_paths.append(os.path.join(os.path.expanduser("~"), SERVICE_ACCOUNT_FILENAME))

    seen = set()
    for p in candidate_paths:
        if not p or p in seen:
            continue
        seen.add(p)
        if os.path.isfile(p):
            return os.path.abspath(p)

    print("DEBUG: looked for service account JSON in these locations:")
    for p in candidate_paths:
        print("  -", p)
    raise FileNotFoundError(f"Service account JSON '{SERVICE_ACCOUNT_FILENAME}' not found. "
                            "Place it next to the script or set SERVICE_ACCOUNT_FILE env var.")

# --------------------------
# Drive helpers (download zip into memory)
# --------------------------
def get_drive_service():
    sa_path = find_service_account_file()
    creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def find_file_by_name(service, name):
    # Find file by exact name in drive root (not guaranteeing uniqueness)
    q = f"""name = '{name.replace("'", "\\'")}' and trashed = false"""
    resp = service.files().list(q=q, pageSize=10, fields="files(id, name, mimeType)").execute()
    files = resp.get("files", [])
    if not files:
        return None
    return files[0]  # first match

def download_file_bytes(service, file_id):
    # Download into BytesIO
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

# --------------------------
# Netex parser (accepts filelike or bytes)
# --------------------------
def parse_netex(xml_path_or_filelike):
    try:
        if isinstance(xml_path_or_filelike, (bytes, bytearray)):
            fh = io.BytesIO(xml_path_or_filelike)
            tree = ET.parse(fh)
        elif hasattr(xml_path_or_filelike, "read"):
            tree = ET.parse(xml_path_or_filelike)
        else:
            tree = ET.parse(xml_path_or_filelike)
        root = tree.getroot()
    except Exception:
        # try wrapping bytes fallback
        if isinstance(xml_path_or_filelike, (bytes, bytearray)):
            fh = io.BytesIO(xml_path_or_filelike)
            tree = ET.parse(fh)
            root = tree.getroot()
        else:
            raise

    ns = {"n": "http://www.netex.org.uk/netex"}

    zone_lookup = {}
    for fz in root.findall(".//n:FareZone", ns):
        fz_id = fz.attrib.get("id")
        name_elem = fz.find("n:Name", ns)
        if fz_id and name_elem is not None and name_elem.text:
            zone_lookup[fz_id] = name_elem.text.strip()

    price_lookup = {}
    for pg in root.findall(".//n:PriceGroup", ns):
        pg_id = pg.attrib.get("id")
        amount_elem = pg.find(".//n:GeographicalIntervalPrice/n:Amount", ns)
        if pg_id and amount_elem is not None and amount_elem.text:
            try:
                price_lookup[pg_id] = f"{float(amount_elem.text.strip()):.2f}"
            except Exception:
                continue

    fares = {}
    for dme in root.findall(".//n:DistanceMatrixElement", ns):
        start_elem = dme.find("n:StartTariffZoneRef", ns)
        end_elem = dme.find("n:EndTariffZoneRef", ns)
        price_ref_elem = dme.find("n:priceGroups/n:PriceGroupRef", ns)
        if start_elem is None or end_elem is None or price_ref_elem is None:
            continue
        start = start_elem.attrib.get("ref")
        end = end_elem.attrib.get("ref")
        price_ref = price_ref_elem.attrib.get("ref")
        price = price_lookup.get(price_ref)
        if start and end and price is not None:
            fares[(start, end)] = price
    return zone_lookup, fares

# --------------------------
# ZIP in-memory loader
# --------------------------
def load_from_fares_zip_in_memory():
    """
    Returns (parsed_data_dict, routes_df, stops_df).
    Downloads Fares.zip into memory and extracts files in memory.
    """
    parsed = {}
    routes_df = pd.DataFrame()
    stops_df = pd.DataFrame()

    try:
        service = get_drive_service()
    except Exception as e:
        print("⚠️ Drive authentication failed:", e)
        traceback.print_exc()
        return parsed, routes_df, stops_df

    f = find_file_by_name(service, ZIP_FILE_NAME_ON_DRIVE)
    if not f:
        print(f"⚠️ '{ZIP_FILE_NAME_ON_DRIVE}' not found on Drive root (or not shared).")
        return parsed, routes_df, stops_df

    print(f"⬇️ Downloading '{ZIP_FILE_NAME_ON_DRIVE}' into memory...")
    try:
        zip_bytes = download_file_bytes(service, f["id"])
    except Exception as e:
        print("⚠️ Failed to download ZIP from Drive:", e)
        traceback.print_exc()
        return parsed, routes_df, stops_df

    # Open zipfile from bytes
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            namelist = zf.namelist()
            print(f"📦 ZIP contains {len(namelist)} entries.")
            for name in namelist:
                # skip directories
                if name.endswith("/"):
                    continue
                basename = os.path.basename(name)
                low = basename.lower()
                try:
                    data = zf.read(name)
                except Exception as e:
                    print(f"⚠️ Could not read {name} from ZIP:", e)
                    continue

                # Handle XMLs
                if low.endswith(".xml"):
                    try:
                        zl, fares = parse_netex(io.BytesIO(data))
                        key = basename[:-4]  # strip .xml
                        parsed[key] = {"zone_lookup": zl, "fares": fares}
                        print(f"✅ Parsed XML: {basename}")
                    except Exception:
                        print(f"⚠️ Failed to parse XML {basename}")
                        traceback.print_exc()
                    continue

                # Handle spreadsheets: Routes.xlsx / Stops.xlsx (accept different name variants)
                if low == "routes.xlsx" or "routes" == os.path.splitext(low)[0]:
                    try:
                        routes_df = pd.read_excel(io.BytesIO(data))
                        print("✅ Loaded Routes.xlsx from ZIP.")
                    except Exception:
                        print(f"⚠️ Failed to load {basename} as Routes.xlsx")
                        traceback.print_exc()
                    continue
                if low == "stops.xlsx" or "stops" == os.path.splitext(low)[0]:
                    try:
                        stops_df = pd.read_excel(io.BytesIO(data))
                        print("✅ Loaded Stops.xlsx from ZIP.")
                    except Exception:
                        print(f"⚠️ Failed to load {basename} as Stops.xlsx")
                        traceback.print_exc()
                    continue

                # Relaxed matches
                if ("routes" in low) and routes_df.empty:
                    try:
                        routes_df = pd.read_excel(io.BytesIO(data))
                        print(f"✅ Loaded Routes (relaxed match) from {basename}.")
                        continue
                    except Exception:
                        pass
                if ("stops" in low) and stops_df.empty:
                    try:
                        stops_df = pd.read_excel(io.BytesIO(data))
                        print(f"✅ Loaded Stops (relaxed match) from {basename}.")
                        continue
                    except Exception:
                        pass

    except zipfile.BadZipFile:
        print("⚠️ The file downloaded is not a valid ZIP.")
        return parsed, routes_df, stops_df
    except Exception:
        traceback.print_exc()
        return parsed, routes_df, stops_df

    return parsed, routes_df, stops_df

# --------------------------
# Embedded Excel fallback loader
# --------------------------
def load_embedded_excel(b64_string):
    if not b64_string:
        return pd.DataFrame()
    try:
        decoded = base64.b64decode(b64_string)
        decompressed = gzip.decompress(decoded)
        bio = io.BytesIO(decompressed)
        return pd.read_excel(bio)
    except Exception:
        traceback.print_exc()
        return pd.DataFrame()

# --------------------------
# Top-level load_data (use ZIP-in-memory, fallback to embedded)
# --------------------------
PARSED_DATA = {}
ROUTES_DF = pd.DataFrame()
STOPS_DF = pd.DataFrame()
LIVE_DATA_LOADED = False

def load_data():
    global PARSED_DATA, ROUTES_DF, STOPS_DF, LIVE_DATA_LOADED
    print("ℹ️ Attempting to load data from Fares.zip on Google Drive (in-memory).")
    parsed, r_df, s_df = load_from_fares_zip_in_memory()

    PARSED_DATA = parsed or {}
    if not r_df.empty and not s_df.empty:
        ROUTES_DF = r_df
        STOPS_DF = s_df
        LIVE_DATA_LOADED = True
        print("✅ Loaded Routes & Stops from Fares.zip (in memory).")
        return

    # fallback
    print("⚠️ Could not load both Routes & Stops from Fares.zip — trying embedded fallbacks.")
    ROUTES_DF = r_df if not r_df.empty else load_embedded_excel(EMBEDDED_ROUTES_B64)
    STOPS_DF = s_df if not s_df.empty else load_embedded_excel(EMBEDDED_STOPS_B64)

    if not ROUTES_DF.empty and not STOPS_DF.empty:
        print("✅ Loaded Routes & Stops from embedded fallback.")
        LIVE_DATA_LOADED = False
        return

    if ROUTES_DF.empty:
        print("⚠️ ROUTES_DF is empty (no Routes.xlsx found in zip and no embedded fallback).")
    if STOPS_DF.empty:
        print("⚠️ STOPS_DF is empty (no Stops.xlsx found in zip and no embedded fallback).")
    LIVE_DATA_LOADED = False

# Load once at startup
load_data()

# --------------------------
# All your helper functions and GUI code (kept the same logic as your working app)
# --------------------------

def route_files_for(route):
    keys = sorted(PARSED_DATA.keys())
    return [k for k in keys if k.startswith(route + " ")]

def faretype_from_key(route, key):
    if key.startswith(route + " "):
        return key[len(route):].strip()
    return None

def route_name_to_service_code(route_name):
    if ROUTES_DF is None or ROUTES_DF.empty:
        return None
    try:
        if ROUTES_DF.shape[1] >= 2:
            mask = ROUTES_DF.iloc[:, 1].astype(str).str.strip() == route_name
            matched = ROUTES_DF.loc[mask]
            if not matched.empty:
                return matched.iloc[0, 0]
    except Exception:
        pass
    return None

def service_code_to_route_name(service_code):
    if ROUTES_DF is None or ROUTES_DF.empty:
        return None
    try:
        if ROUTES_DF.shape[1] >= 2:
            mask = ROUTES_DF.iloc[:, 0].astype(str).str.strip() == str(service_code).strip()
            matched = ROUTES_DF.loc[mask]
            if not matched.empty:
                return matched.iloc[0, 1]
    except Exception:
        pass
    return None

def build_place_to_stage_map_for_service(service_code):
    place_to_stage, stage_to_place = {}, {}
    if STOPS_DF is None or STOPS_DF.empty:
        return place_to_stage, stage_to_place
    try:
        sc_col, stage_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[1], STOPS_DF.columns[7]
    except Exception:
        return place_to_stage, stage_to_place
    mask = STOPS_DF[sc_col].astype(str).str.strip() == str(service_code).strip()
    subset = STOPS_DF.loc[mask]
    for _, row in subset.iterrows():
        stage, place = str(row[stage_col]).strip(), str(row[place_col]).strip()
        if not stage or not place:
            continue
        place_to_stage.setdefault(place, set()).add(stage)
        stage_to_place.setdefault(stage, set()).add(place)
    place_to_stage = {k: sorted(v) for k, v in place_to_stage.items()}
    stage_to_place = {k: sorted(v) for k, v in stage_to_place.items()}
    return place_to_stage, stage_to_place

def get_all_places_from_stops(include_school_services=False):
    if STOPS_DF is None or STOPS_DF.empty:
        return []
    try:
        place_col = STOPS_DF.columns[7]
        sc_col = STOPS_DF.columns[0]
        all_places = STOPS_DF[place_col].dropna().astype(str).str.strip()
        all_places = sorted([p for p in set(all_places) if p and p.lower() != "nan"])
        if include_school_services:
            return all_places
        if ROUTES_DF is None or ROUTES_DF.empty or ROUTES_DF.shape[1] < 3:
            return all_places
        school_col = ROUTES_DF.columns[2]
        non_school_codes = set(
            ROUTES_DF.loc[
                ROUTES_DF[school_col].astype(str).str.lower() != "yes",
                ROUTES_DF.columns[0]
            ].astype(str)
        )
        result = []
        for place in all_places:
            services = set(STOPS_DF.loc[STOPS_DF[place_col] == place, sc_col].astype(str).tolist())
            if services & non_school_codes:
                result.append(place)
        return sorted(result)
    except Exception:
        traceback.print_exc()
        return []

def get_reachable_places(start_place):
    if not start_place or STOPS_DF is None or STOPS_DF.empty:
        return []
    try:
        sc_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[7]
        services = STOPS_DF.loc[STOPS_DF[place_col] == start_place, sc_col].astype(str)
        if services.empty:
            return []
        subset = STOPS_DF.loc[STOPS_DF[sc_col].astype(str).isin(services)]
        reachable = set(subset[place_col].dropna().astype(str).str.strip())
        reachable.discard(start_place)
        return sorted([p for p in reachable if p and p.lower() != "nan"])
    except Exception:
        return []

# --------------------------
# GUI setup
# --------------------------
root = tk.Tk()
root.title("Transdev Fare Finder")
root.geometry("880x640")

route_var = tk.StringVar()
faretype_var = tk.StringVar()
start_place_var = tk.StringVar()
end_place_var = tk.StringVar()
start_stage_var = tk.StringVar()
end_stage_var = tk.StringVar()
fare_var = tk.StringVar(value="Select route and fare type, then places")

zone_lookup, fares, name_to_id = {}, {}, {}

def hide_stage_dropdowns():
    for w in (start_stage_label, start_stage_dropdown, end_stage_label, end_stage_dropdown):
        try:
            w.pack_forget()
        except Exception:
            pass

tk.Label(root, text="Select Route:").pack(pady=(10, 0))
route_dropdown = ttk.Combobox(root, textvariable=route_var, state="readonly", width=80)
route_dropdown.pack(pady=5)

include_schools_var = tk.BooleanVar(value=False)
def has_selectable_faretypes(route_name):
    try:
        files = route_files_for(route_name)
        if not files:
            return False
        for k in files:
            ft = faretype_from_key(route_name, k)
            if ft and ft.strip():
                return True
        return False
    except Exception:
        traceback.print_exc()
        return False

def refresh_route_list():
    try:
        if ROUTES_DF is None or ROUTES_DF.empty or ROUTES_DF.shape[1] < 2:
            route_dropdown["values"] = []
            return
        all_routes = ROUTES_DF.iloc[:, 1].dropna().astype(str).str.strip().unique()
        values = []
        for rn in all_routes:
            if not include_schools_var.get() and ROUTES_DF.shape[1] >= 3:
                matching = ROUTES_DF.loc[ROUTES_DF.iloc[:, 1].astype(str).str.strip() == rn]
                if not matching.empty:
                    if str(matching.iloc[0, 2]).strip().lower() == "yes":
                        continue
            if not has_selectable_faretypes(rn):
                continue
            values.append(rn)
        route_dropdown["values"] = sorted(set(values))
    except Exception as e:
        print("⚠️ Route filter failed:", e)
        traceback.print_exc()
        route_dropdown["values"] = []

def on_include_schools_toggle():
    current_route = route_var.get()
    refresh_route_list()
    vals = route_dropdown["values"]
    if current_route in vals:
        route_var.set(current_route)
    else:
        route_var.set("")
    if route_var.get() and start_place_var.get() and end_place_var.get():
        evaluate_price_options()

include_schools_check = tk.Checkbutton(root, text="Include school services",
                                       variable=include_schools_var,
                                       command=on_include_schools_toggle)
include_schools_check.pack(pady=(0, 6))

refresh_route_list()
route_var.trace_add("write", lambda *a: on_route_selected() )

tk.Label(root, text="Select Fare Type:").pack(pady=(8, 0))
faretype_dropdown = ttk.Combobox(root, textvariable=faretype_var, state="readonly", width=80)
faretype_dropdown.pack(pady=5)
faretype_var.trace_add("write", lambda *a: on_faretype_selected())

tk.Label(root, text="Start Place:").pack(pady=(8, 0))
start_dropdown = ttk.Combobox(root, textvariable=start_place_var, state="readonly", width=80)
start_dropdown.pack(pady=5)
start_dropdown["values"] = get_all_places_from_stops(include_school_services=include_schools_var.get())

tk.Label(root, text="End Place:").pack(pady=(8, 0))
end_dropdown = ttk.Combobox(root, textvariable=end_place_var, state="readonly", width=80)
end_dropdown.pack(pady=5)

start_stage_label = tk.Label(root, text="Select Start Fare Stage (choose if multiple prices):")
start_stage_dropdown = ttk.Combobox(root, textvariable=start_stage_var, state="readonly", width=80)
start_stage_var.trace_add("write", lambda *a: on_stage_selection())

end_stage_label = tk.Label(root, text="Select End Fare Stage (choose if multiple prices):")
end_stage_dropdown = ttk.Combobox(root, textvariable=end_stage_var, state="readonly", width=80)
end_stage_var.trace_add("write", lambda *a: on_stage_selection())

tk.Label(root, textvariable=fare_var, font=("Karbon Bold", 24, "")).pack(pady=(14, 6))

other_routes_var = tk.StringVar(value="")
services_label = tk.Label(root, textvariable=other_routes_var, font=("Karbon Regular", 18))
services_label.pack(pady=(0, 10))

def reset_all():
    route_var.set("")
    faretype_var.set("")
    start_place_var.set("")
    end_place_var.set("")
    start_stage_var.set("")
    end_stage_var.set("")
    fare_var.set("Select route and fare type, then places")
    other_routes_var.set("")
    hide_stage_dropdowns()
    start_dropdown["values"] = get_all_places_from_stops()
    end_dropdown["values"] = []
    refresh_route_list()

reset_button = tk.Button(root, text="Reset", command=reset_all, bg="#d9534f", fg="white", width=12)
reset_button.pack(pady=(8, 6))

def update_end_values():
    start = start_place_var.get()
    route = route_var.get()
    if not start:
        end_dropdown["values"] = []
        return

    reachable = get_reachable_places(start)

    if route:
        service_code = route_name_to_service_code(route)
        if service_code:
            p2s, _ = build_place_to_stage_map_for_service(service_code)
            route_places = sorted([p for p in p2s.keys() if isinstance(p, str) and p.strip() and p.lower() != "nan"])
            reachable = [p for p in reachable if p in route_places]
    else:
        try:
            sc_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[7]
            school_col = ROUTES_DF.columns[2] if ROUTES_DF.shape[1] >= 3 else None

            reachable_filtered = []
            for place in reachable:
                services_to_place = STOPS_DF.loc[STOPS_DF[place_col] == place, sc_col].astype(str).unique().tolist()
                services_from_start = STOPS_DF.loc[STOPS_DF[place_col] == start, sc_col].astype(str).unique().tolist()
                common_services = set(services_to_place) & set(services_from_start)
                if not common_services:
                    continue
                if school_col:
                    non_school_services = ROUTES_DF.loc[
                        ROUTES_DF[ROUTES_DF.columns[0]].astype(str).isin(common_services)
                        & (ROUTES_DF[school_col].astype(str).str.lower() != "yes")
                    ]
                    if not non_school_services.empty:
                        reachable_filtered.append(place)
                else:
                    reachable_filtered.append(place)
            reachable = sorted(reachable_filtered)
        except Exception:
            traceback.print_exc()

    reachable = sorted([p for p in reachable if p != start])
    end_dropdown["values"] = reachable

    if end_place_var.get() and end_place_var.get() not in reachable:
        end_place_var.set("")

def show_other_routes(start_place, end_place):
    try:
        if not (start_place and end_place):
            other_routes_var.set("")
            return
        if STOPS_DF is None or STOPS_DF.empty or ROUTES_DF is None or ROUTES_DF.empty:
            other_routes_var.set("")
            return

        sc_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[7]
        services_start = set(STOPS_DF.loc[STOPS_DF[place_col] == start_place, sc_col].astype(str))
        services_end = set(STOPS_DF.loc[STOPS_DF[place_col] == end_place, sc_col].astype(str))
        common_services = services_start & services_end

        if not common_services:
            other_routes_var.set("")
            return

        route_codes = ROUTES_DF.iloc[:, 0].astype(str)
        school_flags = ROUTES_DF.iloc[:, 2].astype(str).str.lower() if ROUTES_DF.shape[1] >= 3 else pd.Series([""] * len(ROUTES_DF))

        current_route = route_var.get()
        exclude_9 = bool(current_route)

        mask = ROUTES_DF[ROUTES_DF.columns[0]].astype(str).isin(common_services) & (school_flags != "yes")
        if exclude_9:
            mask = mask & (~ROUTES_DF.iloc[:, 0].astype(str).str.startswith("9"))

        if ROUTES_DF.shape[1] >= 4:
            route_numbers = ROUTES_DF.loc[mask, ROUTES_DF.columns[3]].astype(str).tolist()
        elif ROUTES_DF.shape[1] >= 2:
            route_numbers = ROUTES_DF.loc[mask, ROUTES_DF.columns[1]].astype(str).tolist()
        else:
            route_numbers = [str(r) for r in sorted(common_services)]

        if current_route:
            current_service_code = route_name_to_service_code(current_route)
            if current_service_code is not None and ROUTES_DF.shape[1] >= 1:
                current_number = ROUTES_DF.loc[
                    ROUTES_DF.iloc[:, 0].astype(str) == str(current_service_code),
                    ROUTES_DF.columns[3] if ROUTES_DF.shape[1] >= 4 else ROUTES_DF.columns[1]
                ].values
                if len(current_number):
                    route_numbers = [r for r in route_numbers if r != str(current_number[0])]

        route_numbers = sorted(set(route_numbers))
        if route_numbers:
            prefix = "Other services between these places: " if current_route else "Services between these places: "
            other_routes_var.set(prefix + ", ".join(route_numbers))
        else:
            other_routes_var.set("")

    except Exception:
        traceback.print_exc()
        other_routes_var.set("")

def populate_faretypes():
    route_selected = bool(route_var.get())
    start_end_selected = bool(start_place_var.get() and end_place_var.get())
    if not (route_selected or start_end_selected):
        faretype_dropdown["values"] = []
        faretype_var.set("")
        return

    fare_types = set()
    if route_selected:
        route_name = route_var.get()
        files = route_files_for(route_name)
        for f in files:
            ft = faretype_from_key(route_name, f)
            if ft in ("U19 Single", "U19 MySingle", "igo Single"):
                fare_types.add("U19 Single")
            elif ft:
                fare_types.add(ft)
    else:
        start, end = start_place_var.get(), end_place_var.get()
        if STOPS_DF is None or STOPS_DF.empty:
            faretype_dropdown["values"] = []
            faretype_var.set("")
            return
        sc_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[7]
        services_start = set(STOPS_DF.loc[STOPS_DF[place_col] == start, sc_col].astype(str))
        services_end = set(STOPS_DF.loc[STOPS_DF[place_col] == end, sc_col].astype(str))
        common_services = services_start & services_end
        for route_code in common_services:
            route_name = service_code_to_route_name(route_code)
            if not route_name:
                continue
            files = route_files_for(route_name)
            for f in files:
                ft = faretype_from_key(route_name, f)
                if ft in ("U19 Single", "U19 MySingle", "igo Single"):
                    fare_types.add("U19 Single")
                elif ft:
                    fare_types.add(ft)

    faretype_dropdown["values"] = sorted(fare_types)
    if fare_types:
        current = faretype_var.get()
        if current in fare_types:
            faretype_var.set(current)
        elif "Single" in fare_types:
            faretype_var.set("Single")
        else:
            faretype_var.set(sorted(fare_types)[0])
    else:
        faretype_var.set("")

def on_route_selected(*_):
    route = route_var.get()
    faretype_var.set("")
    hide_stage_dropdowns()
    fare_var.set("Select fare type and places")
    if not route:
        start_dropdown["values"] = get_all_places_from_stops()
        update_end_values()
        populate_faretypes()
        return

    matched_keys = route_files_for(route)
    ftypes = []
    for k in matched_keys:
        ft = faretype_from_key(route, k)
        if ft:
            if ft in ("U19 Single", "U19 MySingle", "igo Single"):
                ftypes.append("U19 Single")
            else:
                ftypes.append(ft)
    merged = sorted(set(ftypes))
    faretype_dropdown["values"] = merged

    service_code = route_name_to_service_code(route)
    if not service_code:
        fare_var.set("Service code not found.")
        return
    place_to_stage, _ = build_place_to_stage_map_for_service(service_code)
    places = sorted([p for p in place_to_stage.keys() if isinstance(p, str) and p.strip() and p.lower() != "nan"])
    start_dropdown["values"] = places
    if start_place_var.get() not in places:
        start_place_var.set("")
    update_end_values()

    if start_place_var.get() and end_place_var.get():
        evaluate_price_options()

def on_faretype_selected(*_):
    route, faretype = route_var.get(), faretype_var.get()
    hide_stage_dropdowns()
    if not faretype:
        return

    if route:
        matched = []
        for k in route_files_for(route):
            ft = faretype_from_key(route, k)
            if faretype == "U19 Single" and ft in ("U19 Single", "U19 MySingle", "igo Single"):
                matched.append(k)
            elif ft == faretype:
                matched.append(k)
        if not matched:
            fare_var.set("No fare files found for this route.")
            globals()['zone_lookup'] = {}
            globals()['fares'] = {}
            globals()['name_to_id'] = {}
            return
        zl, fdict, n2i = {}, {}, {}
        for key in matched:
            entry = PARSED_DATA.get(key, {})
            for zid, zname in entry.get("zone_lookup", {}).items():
                zl.setdefault(zid, zname)
                n2i.setdefault(zname, zid)
            for pair, price in entry.get("fares", {}).items():
                fdict.setdefault(pair, price)
        globals()['zone_lookup'] = zl
        globals()['fares'] = fdict
        globals()['name_to_id'] = n2i
        update_end_values()
    else:
        start = start_place_var.get()
        end = end_place_var.get()
        if not (start and end):
            fare_var.set("Select start and end places (or a route) to allow fare lookup.")
            globals()['zone_lookup'] = {}
            globals()['fares'] = {}
            globals()['name_to_id'] = {}
            return

        try:
            sc_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[7]
            services_start = set(STOPS_DF.loc[STOPS_DF[place_col] == start, sc_col].astype(str))
            services_end = set(STOPS_DF.loc[STOPS_DF[place_col] == end, sc_col].astype(str))
            common_services = services_start & services_end
        except Exception:
            common_services = set()

        if not common_services:
            fare_var.set("No services serve both places.")
            globals()['zone_lookup'] = {}
            globals()['fares'] = {}
            globals()['name_to_id'] = {}
            return

        zl, fdict, n2i = {}, {}, {}
        for svc in common_services:
            route_name = service_code_to_route_name(svc)
            if not route_name:
                continue
            for key in route_files_for(route_name):
                ft = faretype_from_key(route_name, key)
                matched_key = None
                if faretype == "U19 Single" and ft in ("U19 Single", "U19 MySingle", "igo Single"):
                    matched_key = key
                elif ft == faretype:
                    matched_key = key
                if matched_key:
                    entry = PARSED_DATA.get(matched_key, {})
                    for zid, zname in entry.get("zone_lookup", {}).items():
                        zl.setdefault(zid, zname)
                        n2i.setdefault(zname, zid)
                    for pair, price in entry.get("fares", {}).items():
                        fdict.setdefault(pair, price)
        globals()['zone_lookup'] = zl
        globals()['fares'] = fdict
        globals()['name_to_id'] = n2i
        update_end_values()

    evaluate_price_options()

def evaluate_price_options(*_):
    route = route_var.get()
    faretype = faretype_var.get()
    start_place = start_place_var.get()
    end_place = end_place_var.get()

    update_end_values()

    start_stage_var.set("")
    end_stage_var.set("")
    hide_stage_dropdowns()

    if start_place and end_place:
        show_other_routes(start_place, end_place)
    else:
        other_routes_var.set("")

    if not (faretype and start_place and end_place):
        if start_place and end_place:
            fare_var.set("Select route or fare type to display fare.")
        else:
            fare_var.set("Select route and fare type, then places")
        return

    global fares, name_to_id

    if route:
        sc = route_name_to_service_code(route)
        if not sc:
            fare_var.set("Service code not found.")
            return
        p2s, _ = build_place_to_stage_map_for_service(sc)
        start_stages, end_stages = p2s.get(start_place, []), p2s.get(end_place, [])
    else:
        try:
            sc_col, place_col = STOPS_DF.columns[0], STOPS_DF.columns[7]
            services_start = set(STOPS_DF.loc[STOPS_DF[place_col] == start_place, sc_col].astype(str))
            services_end = set(STOPS_DF.loc[STOPS_DF[place_col] == end_place, sc_col].astype(str))
            common_services = services_start & services_end
        except Exception:
            common_services = set()

        start_stages_set, end_stages_set = set(), set()
        for svc in common_services:
            p2s, _ = build_place_to_stage_map_for_service(svc)
            start_stages_set.update(p2s.get(start_place, []))
            end_stages_set.update(p2s.get(end_place, []))
        start_stages, end_stages = sorted(start_stages_set), sorted(end_stages_set)

    start_ids = [name_to_id.get(n) for n in start_stages if name_to_id.get(n)]
    end_ids = [name_to_id.get(n) for n in end_stages if name_to_id.get(n)]

    if not start_ids or not end_ids:
        fare_var.set("No matching stages.")
        return

    prices, pm = set(), {}
    for s in start_ids:
        for e in end_ids:
            p = fares.get((s, e)) or fares.get((e, s))
            if p:
                prices.add(p)
                pm[(s, e)] = p

    if not prices:
        fare_var.set("No fare found.")
    elif len(prices) == 1:
        fare_var.set(f"£{list(prices)[0]}")
    else:
        show_start_stage = len(start_stages) > 1
        show_end_stage = len(end_stages) > 1

        start_candidates = [name for name in start_stages if name_to_id.get(name) and any((name_to_id.get(name), e) in pm or (e, name_to_id.get(name)) in pm for e in end_ids)]
        end_candidates = [name for name in end_stages if name_to_id.get(name) and any((s, name_to_id.get(name)) in pm or (name_to_id.get(name), s) in pm for s in start_ids)]

        if len(start_candidates) <= 1:
            show_start_stage = False
        if len(end_candidates) <= 1:
            show_end_stage = False

        if not show_start_stage and not show_end_stage:
            fare_var.set("Multiple fares: " + ", ".join(sorted(prices)))
        else:
            if show_start_stage:
                start_stage_label.pack(pady=(6, 0))
                start_stage_dropdown["values"] = sorted(start_candidates)
                start_stage_dropdown.pack(pady=2)
            if show_end_stage:
                end_stage_label.pack(pady=(6, 0))
                end_stage_dropdown["values"] = sorted(end_candidates)
                end_stage_dropdown.pack(pady=2)
            fare_var.set("Multiple fares found - select specific stage(s) to resolve.")

def on_stage_selection(*_):
    ss, es = start_stage_var.get(), end_stage_var.get()
    s_ids = [name_to_id.get(ss)] if ss and name_to_id.get(ss) else []
    e_ids = [name_to_id.get(es)] if es and name_to_id.get(es) else []
    if not s_ids or not e_ids:
        return
    for s in s_ids:
        for e in e_ids:
            p = fares.get((s, e)) or fares.get((e, s))
            if p:
                fare_var.set(f"£{p}")
                return
    fare_var.set("No fare found for selected stages.")

def on_start_selected(*_):
    update_end_values()
    other_routes_var.set("")
    populate_faretypes()
    if start_place_var.get() and end_place_var.get():
        evaluate_price_options()

def on_end_selected(*_):
    update_end_values()
    other_routes_var.set("")
    populate_faretypes()
    if faretype_var.get():
        evaluate_price_options()

start_place_var.trace_add("write", on_start_selected)
end_place_var.trace_add("write", on_end_selected)
route_var.trace_add("write", lambda *a: on_route_selected())
faretype_var.trace_add("write", lambda *a: on_faretype_selected())

root.mainloop()


# In[ ]:




