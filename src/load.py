from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): The SQLite database connection.
    """

        # TODO: Implementa esta función. Por cada DataFrame en el diccionario, debes
    # usar pandas.DataFrame.to_sql() para cargar el DataFrame en la base de datos
    # como una tabla.
    # Para el nombre de la tabla, utiliza las claves del diccionario `data_frames`.

    
    for table_name, data_frame in data_frames.items():
        try:
            # Guarda el DataFrame en la base de datos usando to_sql()
            data_frame.to_sql(
                name=table_name,  # Nombre de la tabla
                con=database,     # Conexión a la base de datos
                if_exists="replace",  # Reemplaza la tabla si ya existe
                index=False        # Evita guardar el índice como columna
            )
            print(f"Tabla '{table_name}' cargada exitosamente.")
        except Exception as e:
            # Maneja errores durante la carga
            print(f"Error al cargar la tabla '{table_name}': {e}")

