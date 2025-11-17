#!/usr/bin/env python
# coding: utf-8

# In[16]:


# streamlit_app.py
import io
import os
import json
import gzip
import zipfile
import base64
import traceback
import xml.etree.ElementTree as ET
from typing import Dict, Tuple

import pandas as pd
import streamlit as st

# Google libs
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except Exception as e:
    # We'll show a helpful error in the UI rather than crash at import time
    _google_import_error = e
else:
    _google_import_error = None

# --------------------------
# CONFIG
# --------------------------
ZIP_FILE_NAME_ON_DRIVE = "Fares.zip"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# --------------------------
# NETEX parsing (same logic as your script)
# --------------------------
def parse_netex(xml_path_or_filelike) -> Tuple[Dict[str, str], Dict[tuple, str]]:
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
        # try bytes fallback
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
# Drive helpers (download ZIP into memory)
# --------------------------
def get_drive_service_from_info(sa_info: dict):
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def find_file_by_name(service, name):
    q = f"""name = '{name.replace("'", "\\'")}' and trashed = false"""
    resp = service.files().list(q=q, pageSize=10, fields="files(id, name, mimeType)").execute()
    files = resp.get("files", [])
    if not files:
        return None
    return files[0]

def download_file_bytes(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

# --------------------------
# ZIP in-memory loader - returns parsed dict and DataFrames
# --------------------------
def load_from_fares_zip_bytes(zip_bytes: bytes):
    parsed = {}
    routes_df = pd.DataFrame()
    stops_df = pd.DataFrame()

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            namelist = zf.namelist()
            for name in namelist:
                if name.endswith("/"):
                    continue
                basename = os.path.basename(name)
                low = basename.lower()
                try:
                    data = zf.read(name)
                except Exception:
                    continue

                if low.endswith(".xml"):
                    try:
                        zl, fares = parse_netex(io.BytesIO(data))
                        key = basename[:-4]
                        parsed[key] = {"zone_lookup": zl, "fares": fares}
                    except Exception:
                        traceback.print_exc()
                    continue

                # spreadsheet heuristics
                if low == "routes.xlsx" or "routes" == os.path.splitext(low)[0]:
                    try:
                        routes_df = pd.read_excel(io.BytesIO(data))
                    except Exception:
                        traceback.print_exc()
                    continue
                if low == "stops.xlsx" or "stops" == os.path.splitext(low)[0]:
                    try:
                        stops_df = pd.read_excel(io.BytesIO(data))
                    except Exception:
                        traceback.print_exc()
                    continue

                # relaxed matches
                if ("routes" in low) and routes_df.empty:
                    try:
                        routes_df = pd.read_excel(io.BytesIO(data))
                        continue
                    except Exception:
                        pass
                if ("stops" in low) and stops_df.empty:
                    try:
                        stops_df = pd.read_excel(io.BytesIO(data))
                        continue
                    except Exception:
                        pass
    except zipfile.BadZipFile:
        st.error("The downloaded file is not a valid ZIP.")
        return {}, pd.DataFrame(), pd.DataFrame()
    except Exception:
        traceback.print_exc()
        return {}, pd.DataFrame(), pd.DataFrame()

    return parsed, routes_df, stops_df

# --------------------------
# Embedded excel loader (optional)
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
# Helper functions (ported)
# --------------------------
def route_files_for(parsed_data, route):
    keys = sorted(parsed_data.keys())
    return [k for k in keys if k.startswith(route + " ")]

def faretype_from_key(route, key):
    if key.startswith(route + " "):
        return key[len(route):].strip()
    return None

def route_name_to_service_code(routes_df, route_name):
    if routes_df is None or routes_df.empty:
        return None
    try:
        if routes_df.shape[1] >= 2:
            mask = routes_df.iloc[:, 1].astype(str).str.strip() == route_name
            matched = routes_df.loc[mask]
            if not matched.empty:
                return matched.iloc[0, 0]
    except Exception:
        pass
    return None

def service_code_to_route_name(routes_df, service_code):
    if routes_df is None or routes_df.empty:
        return None
    try:
        if routes_df.shape[1] >= 2:
            mask = routes_df.iloc[:, 0].astype(str).str.strip() == str(service_code).strip()
            matched = routes_df.loc[mask]
            if not matched.empty:
                return matched.iloc[0, 1]
    except Exception:
        pass
    return None

def build_place_to_stage_map_for_service(stops_df, service_code):
    place_to_stage, stage_to_place = {}, {}
    if stops_df is None or stops_df.empty:
        return place_to_stage, stage_to_place
    try:
        sc_col, stage_col, place_col = stops_df.columns[0], stops_df.columns[1], stops_df.columns[7]
    except Exception:
        return place_to_stage, stage_to_place
    mask = stops_df[sc_col].astype(str).str.strip() == str(service_code).strip()
    subset = stops_df.loc[mask]
    for _, row in subset.iterrows():
        stage, place = str(row[stage_col]).strip(), str(row[place_col]).strip()
        if not stage or not place:
            continue
        place_to_stage.setdefault(place, set()).add(stage)
        stage_to_place.setdefault(stage, set()).add(place)
    place_to_stage = {k: sorted(v) for k, v in place_to_stage.items()}
    stage_to_place = {k: sorted(v) for k, v in stage_to_place.items()}
    return place_to_stage, stage_to_place

def get_all_places_from_stops(stops_df, routes_df, include_school_services=False):
    if stops_df is None or stops_df.empty:
        return []
    try:
        place_col = stops_df.columns[7]
        sc_col = stops_df.columns[0]
        all_places = stops_df[place_col].dropna().astype(str).str.strip()
        all_places = sorted([p for p in set(all_places) if p and p.lower() != "nan"])
        if include_school_services:
            return all_places
        if routes_df is None or routes_df.empty or routes_df.shape[1] < 3:
            return all_places
        school_col = routes_df.columns[2]
        non_school_codes = set(
            routes_df.loc[
                routes_df[school_col].astype(str).str.lower() != "yes",
                routes_df.columns[0]
            ].astype(str)
        )
        result = []
        for place in all_places:
            services = set(stops_df.loc[stops_df[place_col] == place, sc_col].astype(str).tolist())
            if services & non_school_codes:
                result.append(place)
        return sorted(result)
    except Exception:
        traceback.print_exc()
        return []

def get_reachable_places(stops_df, start_place):
    if not start_place or stops_df is None or stops_df.empty:
        return []
    try:
        sc_col, place_col = stops_df.columns[0], stops_df.columns[7]
        services = stops_df.loc[stops_df[place_col] == start_place, sc_col].astype(str)
        if services.empty:
            return []
        subset = stops_df.loc[stops_df[sc_col].astype(str).isin(services)]
        reachable = set(subset[place_col].dropna().astype(str).str.strip())
        reachable.discard(start_place)
        return sorted([p for p in reachable if p and p.lower() != "nan"])
    except Exception:
        return []

# --------------------------
# UI & state
# --------------------------
st.set_page_config(page_title="NeTEx Fare Finder", layout="wide")
st.title("NeTEx Fare Finder — Streamlit edition")
st.write("This app downloads `Fares.zip` from your Google Drive (shared with the service account) and provides a fare lookup UI.")

# Show helpful error if google libs not installed
if _google_import_error:
    st.error(
        "Google client libraries are not installed in the environment. "
        "Install: `google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib`"
    )
    st.exception(_google_import_error)
    st.stop()

# Provide credential input options
st.sidebar.header("Authentication")
st.sidebar.write(
    "Preferred: store the **service account JSON** in your GitHub repo secrets as "
    "`GOOGLE_SERVICE_ACCOUNT` (Streamlit reads `st.secrets`)."
)
sa_info = None

# Option 1: st.secrets
if "GOOGLE_SERVICE_ACCOUNT" in st.secrets:
    try:
        sa_info = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
        st.sidebar.success("Using GitHub secret: GOOGLE_SERVICE_ACCOUNT")
    except Exception:
        st.sidebar.error("Failed to parse st.secrets['GOOGLE_SERVICE_ACCOUNT'] - ensure it contains the raw JSON text.")
else:
    st.sidebar.info("No GitHub secret found (GOOGLE_SERVICE_ACCOUNT). You can upload credentials.json or set an env var.")
    uploaded = st.sidebar.file_uploader("Upload credentials.json (optional)", type=["json"])
    if uploaded is not None:
        try:
            sa_info = json.load(uploaded)
            st.sidebar.success("Uploaded credentials.json will be used for this session.")
        except Exception:
            st.sidebar.error("Uploaded file is not valid JSON.")

# Option: environment variable (useful for other deployments)
if sa_info is None and os.environ.get("GOOGLE_SERVICE_ACCOUNT"):
    try:
        sa_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
        st.sidebar.success("Using GOOGLE_SERVICE_ACCOUNT environment variable.")
    except Exception:
        st.sidebar.error("Failed to parse GOOGLE_SERVICE_ACCOUNT environment variable.")

# Load / reload controls
reload_button = st.sidebar.button("Reload data from Drive (force)")
load_message = st.sidebar.empty()

@st.cache_data(show_spinner=False)
def load_data_from_drive_cached(sa_info_serialized: str):
    """
    sa_info_serialized: json string of service account info. Cached by this string.
    Returns: parsed_data, routes_df, stops_df, message (str)
    """
    parsed = {}
    routes_df = pd.DataFrame()
    stops_df = pd.DataFrame()
    msg = ""

    try:
        sa_info = json.loads(sa_info_serialized)
        service = get_drive_service_from_info(sa_info)
    except Exception as e:
        msg = f"Drive authentication failed: {e}"
        return parsed, routes_df, stops_df, msg

    f = find_file_by_name(service, ZIP_FILE_NAME_ON_DRIVE)
    if not f:
        msg = f"'{ZIP_FILE_NAME_ON_DRIVE}' not found on Drive root (or not shared with the service account)."
        return parsed, routes_df, stops_df, msg

    try:
        zip_bytes = download_file_bytes(service, f["id"])
    except Exception as e:
        msg = f"Failed to download ZIP from Drive: {e}"
        return parsed, routes_df, stops_df, msg

    parsed, routes_df, stops_df = load_from_fares_zip_bytes(zip_bytes)
    msg = f"Loaded ZIP with {len(parsed)} XML fare files. Routes rows: {len(routes_df)} Stops rows: {len(stops_df)}"
    return parsed, routes_df, stops_df, msg

# container in session state
if "PARSED_DATA" not in st.session_state:
    st.session_state.PARSED_DATA = {}
if "ROUTES_DF" not in st.session_state:
    st.session_state.ROUTES_DF = pd.DataFrame()
if "STOPS_DF" not in st.session_state:
    st.session_state.STOPS_DF = pd.DataFrame()
if "LIVE_DATA_LOADED" not in st.session_state:
    st.session_state.LIVE_DATA_LOADED = False

# Perform load if sa_info is present
if sa_info is not None:
    # Use a deterministic cache key (stringified JSON)
    key = json.dumps(sa_info, sort_keys=True)
    if reload_button:
        # clear cache by calling cached fn with a changed key: append timestamp
        key = key + f"_{st.runtime.scriptrunner.requested_at if hasattr(st.runtime, 'scriptrunner') else ''}"
    parsed, routes_df, stops_df, msg = load_data_from_drive_cached(key)
    st.session_state.PARSED_DATA = parsed or {}
    st.session_state.ROUTES_DF = routes_df
    st.session_state.STOPS_DF = stops_df
    st.session_state.LIVE_DATA_LOADED = bool(not routes_df.empty and not stops_df.empty)
    if msg:
        load_message.info(msg)
else:
    load_message.warning("No credentials provided — the app cannot read Drive. Provide a GitHub secret or upload credentials.json.")

# Provide a manual "Load embedded fallback" (if you have embedded base64 strings you can paste them)
with st.expander("Optional: paste base64(gzip) of Routes/Stops (advanced)"):
    emb_routes = st.text_area("EMBEDDED_ROUTES_B64 (optional)", value="", height=80)
    emb_stops = st.text_area("EMBEDDED_STOPS_B64 (optional)", value="", height=80)
    if st.button("Load embedded Excel fallback"):
        try:
            if emb_routes.strip():
                st.session_state.ROUTES_DF = load_embedded_excel(emb_routes.strip())
            if emb_stops.strip():
                st.session_state.STOPS_DF = load_embedded_excel(emb_stops.strip())
            st.success("Embedded Excel loaded.")
        except Exception:
            st.error("Failed to load embedded content.")

# --------------------------
# UI layout: left controls, right results
# --------------------------
left, right = st.columns([1, 1.4])
with left:
    st.subheader("Lookup controls")
    include_schools = st.checkbox("Include school services", value=False)

    # Build route list (filtered)
    def has_selectable_faretypes(route_name):
        try:
            files = route_files_for(st.session_state.PARSED_DATA, route_name)
            if not files:
                return False
            for k in files:
                ft = faretype_from_key(route_name, k)
                if ft and ft.strip():
                    return True
            return False
        except Exception:
            return False

    def refresh_route_list():
        if st.session_state.ROUTES_DF is None or st.session_state.ROUTES_DF.empty or st.session_state.ROUTES_DF.shape[1] < 2:
            return []
        all_routes = st.session_state.ROUTES_DF.iloc[:, 1].dropna().astype(str).str.strip().unique()
        values = []
        for rn in all_routes:
            if not include_schools and st.session_state.ROUTES_DF.shape[1] >= 3:
                matching = st.session_state.ROUTES_DF.loc[st.session_state.ROUTES_DF.iloc[:, 1].astype(str).str.strip() == rn]
                if not matching.empty:
                    if str(matching.iloc[0, 2]).strip().lower() == "yes":
                        continue
            if not has_selectable_faretypes(rn):
                continue
            values.append(rn)
        return sorted(set(values))

    route_options = refresh_route_list()
    route_choice = st.selectbox("Select Route (optional)", options=[""] + route_options, index=0)

    st.write("Select Fare Type (optional)")
    # fare types built later dynamically
    fare_choice_container = st.empty()

    st.write("Start Place")
    start_choices = get_all_places_from_stops(st.session_state.STOPS_DF, st.session_state.ROUTES_DF, include_school_services=include_schools)
    start_choice = st.selectbox("Start place", options=[""] + start_choices, index=0)

    st.write("End Place")
    # compute end choices dynamically below
    end_choice_container = st.empty()

    # Stage pickers (only show when needed)
    start_stage = None
    end_stage = None

    # Reset button
    if st.button("Reset"):
        # clear selections by rerunning with cleared session values
        try:
            st.session_state.selected_route = ""
            st.session_state.selected_fare = ""
            st.session_state.selected_start = ""
            st.session_state.selected_end = ""
            st.session_state.selected_start_stage = ""
            st.session_state.selected_end_stage = ""
        except Exception:
            pass
        st.experimental_rerun()

with right:
    st.subheader("Fare result")
    fare_text = st.empty()
    other_services_text = st.empty()
    debug_box = st.expander("Debug / Data summary", expanded=False)
    with debug_box:
        st.write("Parsed XML files:", len(st.session_state.PARSED_DATA))
        st.write("Routes rows:", 0 if st.session_state.ROUTES_DF is None else len(st.session_state.ROUTES_DF))
        st.write("Stops rows:", 0 if st.session_state.STOPS_DF is None else len(st.session_state.STOPS_DF))

# --------------------------
# Helper: update end choices based on start and route
# --------------------------
def compute_end_choices(start, route_name, include_schools_flag):
    if not start:
        return []
    reachable = get_reachable_places(st.session_state.STOPS_DF, start)

    if route_name:
        sc = route_name_to_service_code(st.session_state.ROUTES_DF, route_name)
        if sc:
            p2s, _ = build_place_to_stage_map_for_service(st.session_state.STOPS_DF, sc)
            route_places = sorted([p for p in p2s.keys() if isinstance(p, str) and p.strip() and p.lower() != "nan"])
            reachable = [p for p in reachable if p in route_places]
    else:
        try:
            sc_col, place_col = st.session_state.STOPS_DF.columns[0], st.session_state.STOPS_DF.columns[7]
            school_col = st.session_state.ROUTES_DF.columns[2] if st.session_state.ROUTES_DF.shape[1] >= 3 else None

            reachable_filtered = []
            for place in reachable:
                services_to_place = st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == place, sc_col].astype(str).unique().tolist()
                services_from_start = st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == start, sc_col].astype(str).unique().tolist()
                common_services = set(services_to_place) & set(services_from_start)
                if not common_services:
                    continue
                if school_col:
                    non_school_services = st.session_state.ROUTES_DF.loc[
                        st.session_state.ROUTES_DF[st.session_state.ROUTES_DF.columns[0]].astype(str).isin(common_services)
                        & (st.session_state.ROUTES_DF[school_col].astype(str).str.lower() != "yes")
                    ]
                    if not non_school_services.empty:
                        reachable_filtered.append(place)
                else:
                    reachable_filtered.append(place)
            reachable = sorted(reachable_filtered)
        except Exception:
            traceback.print_exc()

    reachable = sorted([p for p in reachable if p != start])
    return reachable

# compute end choices & fare types & stages & final fare logic
# We will store current selections in session_state to persist across reruns
if "selected_route" not in st.session_state:
    st.session_state.selected_route = ""
if "selected_fare" not in st.session_state:
    st.session_state.selected_fare = ""
if "selected_start" not in st.session_state:
    st.session_state.selected_start = ""
if "selected_end" not in st.session_state:
    st.session_state.selected_end = ""
if "selected_start_stage" not in st.session_state:
    st.session_state.selected_start_stage = ""
if "selected_end_stage" not in st.session_state:
    st.session_state.selected_end_stage = ""

# sync controls to session state (prefill)
if route_choice != "" and route_choice != st.session_state.selected_route:
    st.session_state.selected_route = route_choice
if start_choice != "" and start_choice != st.session_state.selected_start:
    st.session_state.selected_start = start_choice

# compute end options
end_options = compute_end_choices(st.session_state.selected_start, st.session_state.selected_route, include_schools)
# present end selectbox (in left column container)
with left:
    end_choice = end_choice_container.selectbox("End place", options=[""] + end_options, index=0)
    if end_choice != "" and end_choice != st.session_state.selected_end:
        st.session_state.selected_end = end_choice

# populate fare types (similar logic)
def compute_fare_types():
    fare_types = set()
    route_selected = bool(st.session_state.selected_route)
    start_end_selected = bool(st.session_state.selected_start and st.session_state.selected_end)
    if not (route_selected or start_end_selected):
        return []
    if route_selected:
        route_name = st.session_state.selected_route
        files = route_files_for(st.session_state.PARSED_DATA, route_name)
        for f in files:
            ft = faretype_from_key(route_name, f)
            if ft in ("U19 Single", "U19 MySingle", "igo Single"):
                fare_types.add("U19 Single")
            elif ft:
                fare_types.add(ft)
    else:
        start, end = st.session_state.selected_start, st.session_state.selected_end
        if st.session_state.STOPS_DF is None or st.session_state.STOPS_DF.empty:
            return []
        sc_col, place_col = st.session_state.STOPS_DF.columns[0], st.session_state.STOPS_DF.columns[7]
        services_start = set(st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == start, sc_col].astype(str))
        services_end = set(st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == end, sc_col].astype(str))
        common_services = services_start & services_end
        for route_code in common_services:
            route_name = service_code_to_route_name(st.session_state.ROUTES_DF, route_code)
            if not route_name:
                continue
            files = route_files_for(st.session_state.PARSED_DATA, route_name)
            for f in files:
                ft = faretype_from_key(route_name, f)
                if ft in ("U19 Single", "U19 MySingle", "igo Single"):
                    fare_types.add("U19 Single")
                elif ft:
                    fare_types.add(ft)
    return sorted(fare_types)

# render fare type control
with left:
    fare_types_list = compute_fare_types()
    if fare_types_list:
        if st.session_state.selected_fare not in fare_types_list:
            st.session_state.selected_fare = fare_types_list[0]
        st.session_state.selected_fare = fare_choice_container.selectbox("Fare type", options=[""] + fare_types_list, index=0 if st.session_state.selected_fare=="" else fare_types_list.index(st.session_state.selected_fare)+1)
    else:
        # show empty
        st.session_state.selected_fare = ""
        fare_choice_container.write("Select start and end (or a route) to see available fare types.")

# When route is changed, adjust start place options to service-specific places
if st.session_state.selected_route:
    svc = route_name_to_service_code(st.session_state.ROUTES_DF, st.session_state.selected_route)
    if svc:
        p2s, _ = build_place_to_stage_map_for_service(st.session_state.STOPS_DF, svc)
        route_places = sorted([p for p in p2s.keys() if isinstance(p, str) and p.strip() and p.lower() != "nan"])
        # if the current selected start is not in route_places, clear it
        if st.session_state.selected_start not in route_places:
            st.session_state.selected_start = ""
            # force rerun (will naturally re-render)
            st.experimental_rerun()

# Evaluate price options based on current state
def evaluate_price_options():
    route = st.session_state.selected_route
    faretype = st.session_state.selected_fare
    start_place = st.session_state.selected_start
    end_place = st.session_state.selected_end

    # If start/end set, show other services
    if start_place and end_place:
        # compute other services text
        try:
            sc_col, place_col = st.session_state.STOPS_DF.columns[0], st.session_state.STOPS_DF.columns[7]
            services_start = set(st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == start_place, sc_col].astype(str))
            services_end = set(st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == end_place, sc_col].astype(str))
            common_services = services_start & services_end
        except Exception:
            common_services = set()

        if not common_services:
            other_services_text.empty()
        else:
            route_codes = st.session_state.ROUTES_DF.iloc[:, 0].astype(str) if st.session_state.ROUTES_DF is not None else pd.Series([])
            school_flags = st.session_state.ROUTES_DF.iloc[:, 2].astype(str).str.lower() if (st.session_state.ROUTES_DF is not None and st.session_state.ROUTES_DF.shape[1] >= 3) else pd.Series([""] * len(st.session_state.ROUTES_DF))
            current_route = route
            exclude_9 = bool(current_route)
            mask = st.session_state.ROUTES_DF[st.session_state.ROUTES_DF.columns[0]].astype(str).isin(common_services) & (school_flags != "yes")
            if exclude_9:
                mask = mask & (~st.session_state.ROUTES_DF.iloc[:, 0].astype(str).str.startswith("9"))

            if st.session_state.ROUTES_DF.shape[1] >= 4:
                route_numbers = st.session_state.ROUTES_DF.loc[mask, st.session_state.ROUTES_DF.columns[3]].astype(str).tolist()
            elif st.session_state.ROUTES_DF.shape[1] >= 2:
                route_numbers = st.session_state.ROUTES_DF.loc[mask, st.session_state.ROUTES_DF.columns[1]].astype(str).tolist()
            else:
                route_numbers = [str(r) for r in sorted(common_services)]

            if current_route:
                current_service_code = route_name_to_service_code(st.session_state.ROUTES_DF, current_route)
                if current_service_code is not None and st.session_state.ROUTES_DF.shape[1] >= 1:
                    current_number = st.session_state.ROUTES_DF.loc[
                        st.session_state.ROUTES_DF.iloc[:, 0].astype(str) == str(current_service_code),
                        st.session_state.ROUTES_DF.columns[3] if st.session_state.ROUTES_DF.shape[1] >= 4 else st.session_state.ROUTES_DF.columns[1]
                    ].values
                    if len(current_number):
                        route_numbers = [r for r in route_numbers if r != str(current_number[0])]
            route_numbers = sorted(set(route_numbers))
            if route_numbers:
                prefix = "Other services between these places: " if current_route else "Services between these places: "
                other_services_text.info(prefix + ", ".join(route_numbers))
            else:
                other_services_text.empty()
    else:
        other_services_text.empty()

    # Fare computation
    if not (faretype and start_place and end_place):
        if start_place and end_place:
            fare_text.info("Select route or fare type to display fare.")
        else:
            fare_text.info("Select route and fare type, then places")
        return

    # prepare zone_lookup, fares, name_to_id aggregated across matched files
    zl, fdict, n2i = {}, {}, {}
    if route:
        matched = []
        for k in route_files_for(st.session_state.PARSED_DATA, route):
            ft = faretype_from_key(route, k)
            if faretype == "U19 Single" and ft in ("U19 Single", "U19 MySingle", "igo Single"):
                matched.append(k)
            elif ft == faretype:
                matched.append(k)
        if not matched:
            fare_text.error("No fare files found for this route.")
            return
        for key in matched:
            entry = st.session_state.PARSED_DATA.get(key, {})
            for zid, zname in entry.get("zone_lookup", {}).items():
                zl.setdefault(zid, zname)
                n2i.setdefault(zname, zid)
            for pair, price in entry.get("fares", {}).items():
                fdict.setdefault(pair, price)
    else:
        # route not selected; find common services and aggregate
        try:
            sc_col, place_col = st.session_state.STOPS_DF.columns[0], st.session_state.STOPS_DF.columns[7]
            services_start = set(st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == start_place, sc_col].astype(str))
            services_end = set(st.session_state.STOPS_DF.loc[st.session_state.STOPS_DF[place_col] == end_place, sc_col].astype(str))
            common_services = services_start & services_end
        except Exception:
            common_services = set()

        if not common_services:
            fare_text.error("No services serve both places.")
            return

        for svc in common_services:
            route_name = service_code_to_route_name(st.session_state.ROUTES_DF, svc)
            if not route_name:
                continue
            for key in route_files_for(st.session_state.PARSED_DATA, route_name):
                ft = faretype_from_key(route_name, key)
                matched_key = None
                if faretype == "U19 Single" and ft in ("U19 Single", "U19 MySingle", "igo Single"):
                    matched_key = key
                elif ft == faretype:
                    matched_key = key
                if matched_key:
                    entry = st.session_state.PARSED_DATA.get(matched_key, {})
                    for zid, zname in entry.get("zone_lookup", {}).items():
                        zl.setdefault(zid, zname)
                        n2i.setdefault(zname, zid)
                    for pair, price in entry.get("fares", {}).items():
                        fdict.setdefault(pair, price)

    # Now compute stages lists
    if route:
        sc = route_name_to_service_code(st.session_state.ROUTES_DF, route)
        if not sc:
            fare_text.error("Service code not found.")
            return
        p2s, _ = build_place_to_stage_map_for_service(st.session_state.STOPS_DF, sc)
        start_stages, end_stages = p2s.get(start_place, []), p2s.get(end_place, [])
    else:
        start_stages_set, end_stages_set = set(), set()
        for svc in common_services:
            p2s, _ = build_place_to_stage_map_for_service(st.session_state.STOPS_DF, svc)
            start_stages_set.update(p2s.get(start_place, []))
            end_stages_set.update(p2s.get(end_place, []))
        start_stages, end_stages = sorted(start_stages_set), sorted(end_stages_set)

    start_ids = [n2i.get(n) for n in start_stages if n2i.get(n)]
    end_ids = [n2i.get(n) for n in end_stages if n2i.get(n)]

    if not start_ids or not end_ids:
        fare_text.error("No matching stages.")
        return

    prices, pm = set(), {}
    for s in start_ids:
        for e in end_ids:
            p = fdict.get((s, e)) or fdict.get((e, s))
            if p:
                prices.add(p)
                pm[(s, e)] = p

    if not prices:
        fare_text.error("No fare found.")
        return
    elif len(prices) == 1:
        fare_text.success(f"£{list(prices)[0]}")
        return
    else:
        # multiple prices - may need stage selection
        show_start_stage = len(start_stages) > 1
        show_end_stage = len(end_stages) > 1

        start_candidates = [name for name in start_stages if n2i.get(name) and any((n2i.get(name), e) in pm or (e, n2i.get(name)) in pm for e in end_ids)]
        end_candidates = [name for name in end_stages if n2i.get(name) and any((s, n2i.get(name)) in pm or (n2i.get(name), s) in pm for s in start_ids)]

        if len(start_candidates) <= 1:
            show_start_stage = False
        if len(end_candidates) <= 1:
            show_end_stage = False

        if not show_start_stage and not show_end_stage:
            fare_text.warning("Multiple fares: " + ", ".join(sorted(prices)))
            return
        else:
            # show selection widgets in the UI (right-side)
            with st.expander("Resolve multiple fares by selecting stages", expanded=True):
                if show_start_stage:
                    ss = st.selectbox("Choose start stage", options=[""] + sorted(start_candidates), index=0, key="selected_start_stage")
                else:
                    ss = ""
                if show_end_stage:
                    es = st.selectbox("Choose end stage", options=[""] + sorted(end_candidates), index=0, key="selected_end_stage")
                else:
                    es = ""
                # when both selected, lookup specific price
                if ((not show_start_stage) or ss) and ((not show_end_stage) or es):
                    s_ids_local = [n2i.get(ss)] if ss and n2i.get(ss) else []
                    e_ids_local = [n2i.get(es)] if es and n2i.get(es) else []
                    if s_ids_local and e_ids_local:
                        for s in s_ids_local:
                            for e in e_ids_local:
                                p = fdict.get((s, e)) or fdict.get((e, s))
                                if p:
                                    fare_text.success(f"£{p}")
                                    return
                        fare_text.error("No fare found for selected stages.")
                        return
            fare_text.info("Multiple fares found - select specific stage(s) to resolve.")
            return

# Trigger fare evaluation whenever selections change
# First sync session selections from controls (left column)
# route_choice, start_choice, end_choice and fare_choice_container control already updated session_state above
# set selected_* from available local variables (if empty keep existing)
if route_choice != "":
    st.session_state.selected_route = route_choice
else:
    # if user cleared route choice in selectbox (selected ""), clear stored route
    st.session_state.selected_route = ""

if start_choice != "":
    st.session_state.selected_start = start_choice

if end_choice != "":
    st.session_state.selected_end = end_choice

# selected fare value (if rendered)
if fare_types_list:
    # obtain the current selected from the widget (via session_state key set earlier)
    # the widget stored value in st.session_state when we created it by using the selectbox with no explicit key
    # we find it at 'Fare type' label? To avoid ambiguity, we just ensure session_state.selected_fare is set to the computed first item if empty
    if st.session_state.selected_fare == "" and fare_types_list:
        st.session_state.selected_fare = fare_types_list[0]

# Evaluate and show results
evaluate_price_options()

# end of app



# In[ ]:




