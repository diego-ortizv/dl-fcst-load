"""COES web scrapper."""
import datetime as dt
import logging
import re
import time
from io import StringIO
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

from downloader.utils import _raise_for_status, get_session

logger = logging.getLogger()

session = get_session()
tz_local = ZoneInfo("America/Lima")

def get_medidores_generacion(start: dt.date, end: dt.date) -> pd.DataFrame:
    """Download final demand.

    Args:
        start (dt.date): Start date object
        end (dt.date): End date object

    Returns:
        pd.DataFrame: Data generation of all generators

    """
    start_str = start.strftime("%d/%m/%Y")
    end_str = end.strftime("%d/%m/%Y")

    res = session.post(
        "https://www.coes.org.pe/Portal/mediciones/medidoresgeneracion/empresas",
        params={"tiposEmpresa": 3},
        timeout=120,
    )
    _raise_for_status(res)

    bs = BeautifulSoup(res.text, "html.parser")

    id_empresas = [
        i.attrs["value"]
        for i in bs.find("select", id="cbEmpresas").find_all("option")
    ]

    res = session.post(
        "https://www.coes.org.pe/Portal/mediciones/medidoresgeneracion/exportar",
        params={
            "fechaInicial": start_str,
            "fechaFinal": end_str,
            "empresas": ",".join(id_empresas),
            "central": 1,
            "parametros": 1,
            "tipo": 3,
        },
        timeout=120,
    )
    _raise_for_status(res)

    res = session.get(
        "https://www.coes.org.pe/Portal/mediciones/medidoresgeneracion/descargar",
        params={"tipo": 3},
        timeout=120,
    )
    _raise_for_status(res)

    data = pd.read_csv(StringIO(res.text), dtype=str)
    data.columns = [col.strip() for col in data.columns.to_list()]
    data["fechahora"] = pd.to_datetime(
        data["fechahora"], format="%d/%m/%Y %H:%M",
    )
    data = data.set_index("fechahora")
    data = data.apply(pd.to_numeric, axis=1, errors="coerce")
    return data.dropna(axis=0, how="all").reset_index()

def get_urls_prog_dia(
    start_date_threshold: dt.date,
) -> list[tuple[dt.date, str, str, str]]:
    """Get download file urls from 'Programa Diario de Operación'.

    Scrapes COES website using Selenium with Chrome backend and fetches
    download urls up until reaching a ``start_date_threshold``

    Args:
        start_date_threshold (dt.date): start threshold

    Returns:
        list[tuple[dt.date, str, str, str]]: Returns a list of tuples with a
        date object (COES date directory where the url belongs), reprogram
        version, filename and download url.

    """
    stop = False
    dowload_urls_map = []

    params = {
        "baseDirectory": "Operación/Programa de Operación/Programa Diario/",
        "url": "Operación/Programa de Operación/Programa Diario/",
        "indicador": "S",
        "initialLink": "Programa de Operación Diario",
        "orderFolder": "D",
    }

    response = session.post(
        "https://www.coes.org.pe/Portal/browser/vistadatos",
        params=params,
    )
    _raise_for_status(response)

    bs = BeautifulSoup(response.text, "html.parser")

    # list of (year_int, year_url)
    year_row_ids = [
        (
            int(year_row.attrs["id"].strip("/").split("/")[-1]),
            year_row.attrs["id"],
        )
        for year_row in bs.find_all("a", class_="infolist-link")
    ]

    # sort by year (desc.)
    year_row_ids = sorted(year_row_ids, key=lambda x: x[0], reverse=True)

    for year_int, year_row_id in year_row_ids:
        if stop:
            break

        # update params year_url
        params["url"] = year_row_id

        response = session.post(
            "https://www.coes.org.pe/Portal/browser/vistadatos",
            params=params,
        )
        _raise_for_status(response)

        bs = BeautifulSoup(response.text, "html.parser")

        # list of (month_int, year_month_url)
        month_row_ids = [
            (
                int(
                    month_row.attrs["id"].strip("/")
                    .split("/")[-1].split("_")[0],
                ),
                month_row.attrs["id"],
            )
            for month_row in bs.find_all("a", class_="infolist-link")
        ]

        # sort by month (desc.)
        month_row_ids = sorted(month_row_ids, key=lambda x: x[0], reverse=True)

        for month_int, month_row_id in month_row_ids:
            if stop:
                break

            params["url"] = month_row_id

            response = session.post(
                "https://www.coes.org.pe/Portal/browser/vistadatos",
                params=params,
            )
            _raise_for_status(response)

            bs = BeautifulSoup(response.text, "html.parser")

            # list of (day_int, year_month_day_url)
            day_row_ids = [
                (
                    int(
                        day_row.attrs["id"].strip("/")
                        .split("/")[-1].split(" ")[-1],
                    ),
                    day_row.attrs["id"],
                )
                for day_row in bs.find_all("a", class_="infolist-link")
            ]

            # sort by day (desc.)
            day_row_ids = sorted(day_row_ids, key=lambda x: x[0], reverse=True)

            for day_int, day_row_id in day_row_ids:
                if stop:
                    break
                params["url"] = day_row_id

                response = session.post(
                    "https://www.coes.org.pe/Portal/browser/vistadatos",
                    params=params,
                )
                _raise_for_status(response)

                bs = BeautifulSoup(response.text, "html.parser")

                date_obj = dt.date(
                    year=year_int,
                    month=month_int,
                    day=day_int,
                )
                if date_obj < start_date_threshold:
                    stop = True
                    break

                msg = f"Listing files of {date_obj}"
                logger.info(msg)

                for link in bs.find_all("input", {"name": "cbSelect"}):
                    download_url = link["value"]
                    file_name = download_url.split("/")[-1]

                    dowload_urls_map.append(
                        (date_obj, file_name, download_url),
                    )
    return dowload_urls_map

def get_urls_reprog_dia(  # noqa: C901
    start_date_threshold: dt.date,
) -> list[tuple[dt.date, str, str, str]]:
    """Get download file urls from 'Reprograma Diario de Operación'.

    Scrapes COES website using Selenium with Chrome backend and fetches
    download urls up until reaching a ``start_date_threshold``

    Args:
        start_date_threshold (dt.date): start threshold

    Returns:
        list[tuple[dt.date, str, str, str]]: Returns a list of tuples with a
        date object (COES date directory where the url belongs), reprogram
        version, filename and download url.

    """
    stop = False
    dowload_urls_map = []

    params = {
        "baseDirectory":
            "Operación/Programa de Operación/Reprograma Diario Operación/",
        "url":
            "Operación/Programa de Operación/Reprograma Diario Operación/",
        "indicador": "S",
        "initialLink": "Reprograma Diario de Operación",
        "orderFolder": "D",
    }

    response = session.post(
        "https://www.coes.org.pe/Portal/browser/vistadatos",
        params=params,
    )
    _raise_for_status(response)

    bs = BeautifulSoup(response.text, "html.parser")

    # list of (year_int, year_url)
    year_row_ids = [
        (
            int(year_row.attrs["id"].strip("/").split("/")[-1]),
            year_row.attrs["id"],
        )
        for year_row in bs.find_all("a", class_="infolist-link")
    ]

    # sort by year (desc.)
    year_row_ids = sorted(year_row_ids, key=lambda x: x[0], reverse=True)

    for year_int, year_row_id in year_row_ids:
        if stop:
            break

        # update params year_url
        params["url"] = year_row_id

        response = session.post(
            "https://www.coes.org.pe/Portal/browser/vistadatos",
            params=params,
        )
        _raise_for_status(response)

        bs = BeautifulSoup(response.text, "html.parser")

        # list of (month_int, year_month_url)
        month_row_ids = [
            (
                int(
                    month_row.attrs["id"].strip("/")
                    .split("/")[-1].split("_")[0],
                ),
                month_row.attrs["id"],
            )
            for month_row in bs.find_all("a", class_="infolist-link")
        ]

        # sort by month (desc.)
        month_row_ids = sorted(month_row_ids, key=lambda x: x[0], reverse=True)

        for month_int, month_row_id in month_row_ids:
            if stop:
                break

            params["url"] = month_row_id

            response = session.post(
                "https://www.coes.org.pe/Portal/browser/vistadatos",
                params=params,
            )
            _raise_for_status(response)

            bs = BeautifulSoup(response.text, "html.parser")

            # list of (day_int, year_month_day_url)
            day_row_ids = [
                (
                    int(
                        day_row.attrs["id"].strip("/")
                        .split("/")[-1].split(" ")[-1]
                    ),
                    day_row.attrs["id"],
                )
                for day_row in bs.find_all("a", class_="infolist-link")
            ]

            # sort by day (desc.)
            day_row_ids = sorted(day_row_ids, key=lambda x: x[0], reverse=True)

            for day_int, day_row_id in day_row_ids:
                if stop:
                    break
                params["url"] = day_row_id

                response = session.post(
                    "https://www.coes.org.pe/Portal/browser/vistadatos",
                    params=params,
                )
                _raise_for_status(response)

                bs = BeautifulSoup(response.text, "html.parser")

                date_obj = dt.date(
                    year=year_int,
                    month=month_int,
                    day=day_int,
                )

                if date_obj < start_date_threshold:
                    stop = True
                    break

                # list of (reprog_version (str), year_month_day_reprog_url)
                reprog_row_ids = [
                    (
                        re.sub(
                            r"[^A-Za-z]",
                            "",
                            reprog_row.attrs["id"]
                            .strip("/")
                            .split("/")[-1]
                            .split(" ")[-1],
                        ),
                        reprog_row.attrs["id"],
                    )
                    for reprog_row in bs.find_all("a", class_="infolist-link")
                ]

                # sort by reprog (desc. F -> A)
                reprog_row_ids = sorted(
                    reprog_row_ids,
                    key=lambda x: x[0],
                    reverse=True,
                )

                for reprog_version, reprog_row_id in reprog_row_ids:
                    if stop:
                        break
                    params["url"] = reprog_row_id

                    response = session.post(
                        "https://www.coes.org.pe/Portal/browser/vistadatos",
                        params=params,
                    )
                    _raise_for_status(response)

                    bs = BeautifulSoup(response.text, "html.parser")

                    msg = f"Listing files of {date_obj} - {reprog_version}"
                    logger.info(msg)

                    for link in bs.find_all("input", {"name": "cbSelect"}):
                        download_url = link["value"]
                        file_name = download_url.split("/")[-1]

                        dowload_urls_map.append(
                            (date_obj, reprog_version, file_name, download_url)
                        )
    return dowload_urls_map

def get_demanda_ejecutado(
    start: dt.date,
    end: dt.date,
) -> requests.Response|None:
    """Get data from 'Demanda Ejecutado' endpoint.

    Fetch load system readings (30min granularity), daily and weekly expected
    demand from ``start`` to ``end`` (inclusive)

    Note: No timezone spec is required (API endpoint assumes America/Lima date)

    Args:
        start (datetime.date): start date object
        end (datetime.date): end date object

    Returns:
        pd.DataFrame: Resulting timeseries from ``start`` to ``end`` dates

    """
    msg_info = f"Requesting 'demanda ejecutado' from {start} to {end}"
    logger.info(msg_info)
    try:
        start_str = start.strftime("%d/%m/%Y")
        end_str = end.strftime("%d/%m/%Y")

        response_demanda_ejecutado = session.post(
            url="https://www.coes.org.pe/Portal/portalinformacion/Demanda",
            params={
                "fechaInicial": start_str,
                "fechaFinal": end_str,
            },
            timeout=90,
        )
        _raise_for_status(response_demanda_ejecutado)
    except Exception as e:
        msg_exc = f"Exception while requesting 'demanda ejecutado':\n{str(e)}"
        logger.exception(msg_exc)
        return None
    else:
        msg_info = ("Successfully fetched 'demanda ejecutado'. "
                    "Elapsed time of request: "
                    f"{response_demanda_ejecutado.elapsed.total_seconds():.1f}"
                    " seconds")
        logger.info(msg_info)

        return response_demanda_ejecutado

def process_demanda_ejecutado(
    response_demanda_ejecutado: requests.Response,
) -> pd.DataFrame|None:
    """Processs demanda ejecutado."""
    msg_info = "Processing 'demanda ejecutado'"
    logger.info(msg_info)
    try:
        et = time.time()

        response_demanda_ejecutado_json_obj = response_demanda_ejecutado.json()
        demanda_ejecutado_json = response_demanda_ejecutado_json_obj["Data"]

        df = pd.DataFrame.from_records(demanda_ejecutado_json)

        # Cast str to datetime and localize
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%Y/%m/%d %H:%M")
        df["Fecha"] = df["Fecha"].dt.tz_localize(tz_local)

    except Exception as e:
        msg_exc = f"Exception while processing 'demanda ejecutado':\n{str(e)}"
        logger.exception(msg_exc)
        return None
    else:
        et = time.time() - et
        msg_info = ("Successfully processed 'demanda ejecutado'. "
                    f"Elapsed time of process: {et:.1f} seconds")
        logger.info(msg_info)
        return df
