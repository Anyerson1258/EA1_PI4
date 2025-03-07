from typing import Dict

import requests
from pandas import DataFrame, read_csv, read_json, to_datetime

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): URL to the public holidays.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """

       # TODO: Implementa esta función.
    # Debes usar la biblioteca requests para obtener los días festivos públicos del año dado.
    # La URL es public_holidays_url/{year}/BR.
    # Debes eliminar las columnas "types" y "counties" del DataFrame.
    # Debes convertir la columna "date" a datetime.
    # Debes lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status
    # de la biblioteca requests.
    
    try:
        # Realiza la solicitud a la API
        response = requests.get(f"{public_holidays_url}/{year}/BR")
        response.raise_for_status()  # Lanzar una excepción si hay un error en la solicitud
        
        # Conviertir la respuesta JSON en un DataFrame
        holidays = read_json(response.text)
        
        # Eliminar las columnas innecesarias
        holidays = holidays.drop(columns=["types", "counties"])
        
        # Conviertir la columna "date" a datetime
        holidays["date"] = to_datetime(holidays["date"])
        
        return holidays
    except requests.RequestException as e:
        # Manejar errores de la solicitud
        print(f"Error fetching public holidays: {e}")
        raise SystemExit(e)

def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    # Carga los DataFrames desde los archivos CSV
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    # Obtiene los días festivos y los añade al diccionario
    holidays = get_public_holidays(public_holidays_url, "2017")
    dataframes["public_holidays"] = holidays

    return dataframes

