"""Conexión a PostgreSQL y funciones reutilizables para ejecutar sentencias."""
import contextlib
import logging
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)


def get_connection():
    """Abre y devuelve una conexión a PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def execute(
    query: str,
    params: tuple | dict | None = None,
    *,
    commit: bool = True,
) -> int:
    """
    Ejecuta una sentencia (INSERT, UPDATE, DELETE, DDL).
    Devuelve el número de filas afectadas.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            rowcount = cur.rowcount
        if commit:
            conn.commit()
    return rowcount


def fetch_one(query: str, params: tuple | dict | None = None) -> dict[str, Any] | None:
    """Ejecuta un SELECT y devuelve la primera fila como diccionario, o None si no hay resultados."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchone()


def fetch_all(query: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    """Ejecuta un SELECT y devuelve todas las filas como lista de diccionarios."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


def execute_many(
    query: str,
    params_list: list[tuple] | list[dict],
    *,
    commit: bool = True,
) -> int:
    """Ejecuta la misma sentencia con múltiples conjuntos de parámetros. Devuelve el total de filas afectadas."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, params_list)
            total = cur.rowcount
        if commit:
            conn.commit()
    return total


@contextlib.contextmanager
def transaction():
    """
    Context manager para ejecutar varias sentencias en una sola transacción.
    Hace commit al salir con éxito y rollback en caso de excepción.

    Uso:
        with db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO ...")
                cur.execute("UPDATE ...")
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_sql_script(script: str, *, commit: bool = True) -> None:
    """
    Ejecuta un script SQL (varias sentencias separadas por ;).
    Útil para migraciones o DDL.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(script)
        if commit:
            conn.commit()


def call_function(
    func_name: str,
    args: tuple | list | None = None,
    *,
    commit: bool = True,
) -> Any:
    """
    Llama a una función de PostgreSQL y devuelve su resultado.

    Ejemplo: call_function("mi_funcion", (arg1, arg2))
    Para función sin argumentos: call_function("mi_funcion")
    """
    args = list(args) if args else []
    with get_connection() as conn:
        with conn.cursor() as cur:
            if args:
                q = sql.SQL("SELECT * FROM {}({})").format(
                    sql.Identifier(func_name),
                    sql.SQL(", ").join([sql.Placeholder()] * len(args)),
                )
                cur.execute(q, args)
            else:
                cur.execute(
                    sql.SQL("SELECT * FROM {}()").format(sql.Identifier(func_name)),
                )
            out = cur.fetchone()
        if commit:
            conn.commit()
    # Si la función devuelve un solo valor, devolverlo; si no, el diccionario completo
    if out and len(out) == 1:
        return next(iter(out.values()))
    return out
