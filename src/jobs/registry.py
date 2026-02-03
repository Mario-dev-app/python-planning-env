"""Registro de tareas: nombre -> función ejecutable."""
import logging
from typing import Callable

logger = logging.getLogger(__name__)

# Mapa: nombre_tarea -> función sin argumentos
registry: dict[str, Callable[[], None]] = {}


def register(name: str):
    """Decorador para registrar una función como tarea programable."""

    def decorator(func: Callable[[], None]):
        registry[name] = func
        return func

    return decorator


def get_job(name: str) -> Callable[[], None] | None:
    """Obtiene la función de una tarea por nombre."""
    return registry.get(name)


def list_jobs() -> list[str]:
    """Lista los nombres de las tareas registradas."""
    return list(registry.keys())
